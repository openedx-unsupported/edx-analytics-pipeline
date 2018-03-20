"""
Support for loading data into an HP Vertica database.
"""

import logging
import traceback
from collections import namedtuple

import luigi
import luigi.configuration

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL
from edx.analytics.tasks.util.vertica_target import CredentialFileVerticaTarget

log = logging.getLogger(__name__)

try:
    import vertica_python
    from vertica_python.errors import QueryError
    vertica_client_available = True  # pylint: disable=invalid-name
except ImportError:
    log.warn('Unable to import Vertica client libraries')
    # On hadoop slave nodes we don't have Vertica client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    vertica_client_available = False  # pylint: disable=invalid-name

PROJECTION_TYPE_NORMAL = 'Normal'
PROJECTION_TYPE_AGGREGATE = 'Aggregate'

VerticaProjection = namedtuple('VerticaProjection',  # pylint: disable=invalid-name
                               ['name', 'type', 'definition', ])


class VerticaCopyTaskMixin(OverwriteOutputMixin):
    """
    Parameters for copying a database into Vertica.

    """
    schema = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    read_timeout = luigi.IntParameter(
        config_path={'section': 'vertica-export', 'name': 'read_timeout'}
    )
    marker_schema = luigi.Parameter(
        default=None,
        description='The marker schema to which to write the marker table. marker_schema would '
        'default to the schema value if the value here is None.'
    )

    persistent_schema = luigi.Parameter(
        default='experimental',
        config_path={'section': 'vertica-export', 'name': 'persistent_schema'}
    )


class VerticaCopyTask(VerticaCopyTaskMixin, luigi.Task):
    """
    A task for copying into a Vertica database.

    Note that the default behavior if overwrite is true is to first delete the existing
    contents of the table being written to and then delete the entire history of table
    updates corresponding to writes to that table, not just table updates with the same
    update id (i.e. updates corresponding to writes done by a task with the same exact
    task name and set of parameters).

    Overwrite init_copy and init_touch if you want a different overwrite behavior in a
    subclass.
    """
    required_tasks = None
    output_target = None

    restricted_roles = luigi.ListParameter(
        config_path={'section': 'vertica-export', 'name': 'restricted_roles'},
        default=[],
        description='List of roles to which to provide access when a database column is marked as restricted.'
    )

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
                'insert_source': self.insert_source_task
            }
        return self.required_tasks

    @property
    def insert_source_task(self):
        """Defines the task that provides source of data for Vertica bulk loading."""
        raise NotImplementedError

    @property
    def table(self):
        """Provides the name of the database table."""
        raise NotImplementedError

    @property
    def columns(self):
        """
        Provides definition of columns.  If only writing to existing tables, then columns() need only provide a list of
        names.

        If also needing to create the table, then columns() should define a list of (name, definition) tuples.
        For example, ('first_name', 'VARCHAR(255)').
        """
        raise NotImplementedError

    @property
    def auto_primary_key(self):
        """Tuple defining name and definition of an auto-incrementing primary key, or None."""
        return ('id', 'AUTO_INCREMENT')

    @property
    def foreign_key_mapping(self):
        """Dictionary of column_name: (schema.table, column) pairs representing foreign key constraints."""
        return {}

    @property
    def table_partition_key(self):
        """String defining expression to use for partitioning data in Vertica table, or None."""
        return None

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return [('created', 'TIMESTAMP DEFAULT NOW()')]

    @property
    def unique_columns(self):
        """
        List of tuples, each containing column or group of columns on which the UNIQUE constraint would be added.
        Example: [('c1',), ('c1, 'c2',)]
        """
        return []

    @property
    def projections(self):
        """Provides projection definitions to use after table creation and initialization to create projections.

        Return value should be a list of VerticaProjection namedtuple objects.  The name and
        definition fields may be templates using "{schema}" and "{table}".
        """
        return []

    def create_schema(self, connection):
        """
        Override to provide code for creating the target schema, if not existing.

        By default it will be created using types (optionally) specified in columns.

        If overridden, use the provided connection object for setting
        up the schema in order to create the schema and insert data
        using the same transaction.
        """
        query = "CREATE SCHEMA IF NOT EXISTS {schema}".format(schema=self.schema)
        log.debug(query)
        connection.cursor().execute(query)

    def create_column_definitions(self):
        """
        Builds the list of column definitions for the table to be loaded.

        Assumes that columns are specified as (name, definition) tuples.

        :return a string to be used in a SQL query to create the table
        """
        columns = []
        if self.auto_primary_key is not None:
            columns.append(self.auto_primary_key)
        columns.extend(self.columns)
        if self.default_columns is not None:
            columns.extend(self.default_columns)
        if self.auto_primary_key is not None:
            columns.append(("PRIMARY KEY", "({name})".format(name=self.auto_primary_key[0])))

        coldefs = ','.join(
            '{name} {definition}'.format(name=name, definition=definition) for name, definition in columns
        )
        return coldefs

    def create_table(self, connection):
        """
        Override to provide code for creating the target table, if not existing.

        Requires the schema to exist first.

        By default it will be created using types (optionally) specified in columns.
        If overridden, use the provided connection object for setting
        up the table in order to create the table and insert data
        using the same transaction.
        """

        if len(self.columns[0]) != 2:
            # only names of columns specified, no types
            raise NotImplementedError(
                "create_table() not implemented for %r and columns types not specified"
                % self.table
            )

        # Assumes that columns are specified as (name, definition) tuples
        coldefs = self.create_column_definitions()

        foreign_key_defs = ''
        for column in self.foreign_key_mapping:
            foreign_key_defs += ", FOREIGN KEY ({col}) REFERENCES {other_schema_and_table} ({other_col})".format(
                col=column, other_schema_and_table=self.foreign_key_mapping[column][0],
                other_col=self.foreign_key_mapping[column][1]
            )

        partition_key_def = ''
        if self.table_partition_key:
            partition_key_def = ' PARTITION BY {key}'.format(key=self.table_partition_key)

        unique_constraints_def = ''
        for columns in self.unique_columns:
            unique_constraints_def += ", UNIQUE({cols})".format(cols=', '.join(columns))

        query = "CREATE TABLE IF NOT EXISTS {schema}.{table} ({coldefs}{foreign_key_defs}{unique_constraints_def}){partition_key_def}".format(
            schema=self.schema, table=self.table, coldefs=coldefs, foreign_key_defs=foreign_key_defs,
            unique_constraints_def=unique_constraints_def, partition_key_def=partition_key_def,
        )
        log.debug(query)
        connection.cursor().execute(query)

    def _get_aggregate_projections(self):
        """Get projections that are aggregates, and fill in values."""
        return [
            VerticaProjection
            (
                template.name.format(schema=self.schema, table=self.table),
                template.type,
                template.definition.format(schema=self.schema, table=self.table),
            ) for template in self.projections if template.type == PROJECTION_TYPE_AGGREGATE
        ]

    def _get_nonaggregate_projections(self):
        """Get projections that are not aggregates, and fill in values."""
        return [
            VerticaProjection
            (
                template.name.format(schema=self.schema, table=self.table),
                template.type,
                template.definition.format(schema=self.schema, table=self.table),
            ) for template in self.projections if template.type != PROJECTION_TYPE_AGGREGATE
        ]

    def drop_aggregate_projections(self, connection):
        """
        Drop any projections that are aggregates.

        Aggregate projections must be removed from a table before its contents can be deleted.
        """
        for projection in self._get_aggregate_projections():
            query = "DROP PROJECTION IF EXISTS {name};".format(name=projection.name)
            log.debug(query)
            connection.cursor().execute(query)

    def create_aggregate_projections(self, connection):
        """
        Define all aggregate projections on table.
        """
        projections = self._get_aggregate_projections()
        for projection in projections:
            query = "CREATE PROJECTION IF NOT EXISTS {name} {definition};".format(
                name=projection.name, definition=projection.definition
            )
            log.debug(query)
            connection.cursor().execute(query)

        # If any projections were created, start a refresh as well.
        if len(projections) > 0:
            query = 'SELECT start_refresh();'
            log.debug(query)
            connection.cursor().execute(query)

    def create_nonaggregate_projections(self, connection):
        """
        Define all projections on table.
        """
        for projection in self._get_nonaggregate_projections():
            query = "CREATE PROJECTION IF NOT EXISTS {name} {definition};".format(
                name=projection.name, definition=projection.definition
            )
            log.debug(query)
            connection.cursor().execute(query)

    def purge_deleted_records(self, connection):
        """
        Rewrite the table on disk to remove any deleted records that are no longer in use.
        """
        # TODO: doing a bulk delete + purge is an anti-pattern in Vertica! We should change our strategy.
        if self.overwrite:
            query = "SELECT PURGE_TABLE('{schema}.{table}')".format(
                schema=self.schema, table=self.table
            )
            log.debug(query)
            connection.cursor().execute(query)

    def update_id(self):
        """This update id will be a unique identifier for this insert on this table."""
        # For MySQL tasks, we take the hash of the task id, but since Vertica does not similarly
        # limit the size of columns, we can safely use the entire task ID.
        return str(self)

    def output(self):
        """
        Returns a VerticaTarget representing the inserted dataset.

        Normally you don't override this.
        """
        if self.output_target is None:
            self.output_target = CredentialFileVerticaTarget(
                credentials_target=self.input()['credentials'],
                table=self.table,
                schema=self.schema,
                update_id=self.update_id(),
                read_timeout=self.read_timeout,
                marker_schema=self.marker_schema,
            )

        return self.output_target

    def init_copy(self, connection):
        """
        Override to perform custom queries.

        Because attempting a DELETE will give this transaction an exclusive
        lock on the marker table (Vertica does not support row-level locking),
        the pre-copy initialization only overwrites the table to be overwritten
        and not the relevant rows of the marker table, which are deleted
        immediately before the marker table is marked with success for this
        task.  This way, an exclusive lock on the marker table is held for only
        a very brief duration instead of the entire course of the data copy,
        which might in general take longer than the 5 minutes that Vertica is
        willing to wait for a lock before giving up and throwing an error.

        Any code here will be formed in the same transaction as the
        main copy, just prior to copying data. Example use cases
        include truncating the table or removing all data older than X
        in the database to keep a rolling window of data available in
        the table.

        Note that this method acquires an exclusive (X) lock on the table
        this task is going to write to for the remainder of the transaction
        (i.e. until a commit or rollback).
        """
        # clear table contents
        self.attempted_removal = True
        if self.overwrite:
            # Before changing the current contents table, we have to make sure there
            # are no aggregate projections on it.
            self.drop_aggregate_projections(connection)

            # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
            # commit the currently open transaction before continuing with the copy.
            query = "DELETE FROM {schema}.{table}".format(schema=self.schema, table=self.table)
            log.debug(query)
            connection.cursor().execute(query)

        # vertica-python and its maintainers intentionally avoid supporting open
        # transactions like we do when self.overwrite=True (DELETE a bunch of rows
        # and then COPY some), per https://github.com/uber/vertica-python/issues/56.
        # The DELETE commands in this method will cause the connection to see some
        # messages that will prevent it from trying to copy any data (if the cursor
        # successfully executes the DELETEs), so we flush the message buffer.
        connection.cursor().flush_to_query_ready()

    def init_touch(self, connection):
        """
        Clear the relevant rows from the marker table before touching
        it to denote that the task has been completed.

        Note that this method acquires an exclusive (X) lock on the
        marker table for the remainder of the transaction (i.e. until
        a commit or rollback).
        """
        if self.overwrite:
            # Clear the appropriate rows from the luigi Vertica marker table
            marker_table = self.output().marker_table  # side-effect: sets self.output_target if it's None
            marker_schema = self.output().marker_schema
            try:
                query = "DELETE FROM {marker_schema}.{marker_table} where target_table='{schema}.{target_table}';".format(
                    schema=self.schema,
                    marker_schema=marker_schema,
                    marker_table=marker_table,
                    target_table=self.table,
                )
                log.debug(query)
                connection.cursor().execute(query)
            except vertica_python.errors.Error as err:
                if (type(err) is vertica_python.errors.MissingRelation) or ('Sqlstate: 42V01' in err.args[0]):
                    # If so, then our query error failed because the table doesn't exist.
                    pass
                else:
                    raise

        # vertica-python and its maintainers intentionally avoid supporting open
        # transactions like we do when self.overwrite=True (DELETE a bunch of rows
        # and then COPY some), per https://github.com/uber/vertica-python/issues/56.
        # The DELETE commands in this method will cause the connection to see some
        # messages that will prevent it from trying to copy any data (if the cursor
        # successfully executes the DELETEs), so we flush the message buffer.
        connection.cursor().flush_to_query_ready()

    @property
    def copy_delimiter(self):
        """The delimiter in the data to be copied.  Default is tab (\t)"""
        return "E'\t'"

    @property
    def copy_null_sequence(self):
        """The null sequence in the data to be copied.  Default is Hive NULL (\\N)"""
        return "'\\N'"

    @property
    def copy_enclosed_by(self):
        """The field's enclosing character. Default is empty string."""
        return "''"

    @property
    def copy_escape_spec(self):
        """
        The escape character to use to have special characters be treated literally.

        Copy's default is backslash if this is a zero-length string.   To disable escaping,
        use "NO ESCAPE".  To use a different character, use "ESCAPE AS 'char'".


        """
        return ""

    def copy_data_table_from_target(self, cursor):
        """Performs the copy query from the insert source."""
        if isinstance(self.columns[0], basestring):
            column_names = ','.join([name for name in self.columns])
        elif len(self.columns[0]) == 2:
            column_names = ','.join([name for name, _type in self.columns])
        else:
            raise Exception('columns must consist of column strings or '
                            '(column string, type string) tuples (was %r ...)'
                            % (self.columns[0],))

        try:
            with self.input()['insert_source'].open('r') as insert_source_file:
                log.debug("Running stream copy from source file")
                cursor.copy(
                    "COPY {schema}.{table} ({cols}) FROM STDIN ENCLOSED BY {enclosed_by} DELIMITER AS {delim} NULL AS {null} {escape_spec} DIRECT ABORT ON ERROR NO COMMIT;".format(
                        schema=self.schema,
                        table=self.table,
                        cols=column_names,
                        delim=self.copy_delimiter,
                        null=self.copy_null_sequence,
                        enclosed_by=self.copy_enclosed_by,
                        escape_spec=self.copy_escape_spec,
                    ),
                    insert_source_file
                )
                log.debug("Finished stream copy from source file")
        except RuntimeError:
            # While calling finish on an input target, Luigi throws a RuntimeError exception if the subprocess command
            # to read the input returns a non-zero return code. As all of the data's been read already, we choose to ignore
            # this exception.
            traceback_str = traceback.format_exc()
            if "self._finish()" in traceback_str:
                log.debug("Luigi raised RuntimeError while calling _finish on input target.")
            else:
                raise

    def analyze_constraints(self, cursor):
        # Vertica does not check for constraint violations during data loading.
        # We explicitly check for violations by calling ANALYZE_CONSTRAINTS function, and fail
        # the workflow if a violation is found.
        if self.foreign_key_mapping or self.unique_columns:
            query = "SELECT ANALYZE_CONSTRAINTS('{schema}.{table}')".format(
                schema=self.schema,
                table=self.table
            )
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                raise Exception('{type} key violation on table: {schema}.{table} with column values:{val}'.
                                format(type=row[4], schema=row[0], table=row[1], val=row[5]))

    @property
    def restricted_columns(self):
        return []

    def create_access_policies(self, connection):
        cursor = connection.cursor()
        for column in self.restricted_columns:
            all_restricted_roles = ['dbadmin'] + list(self.restricted_roles)
            expression = ' OR '.join(["ENABLED_ROLE('{0}')".format(role) for role in all_restricted_roles])
            statement = """
CREATE ACCESS POLICY ON {schema}.{table} FOR COLUMN {column}
CASE WHEN {expression} THEN {column}
ELSE 'Restricted'
END
ENABLE;""".format(schema=self.schema, table=self.table, column=column, expression=expression)
            log.debug(statement)
            try:
                cursor.execute(statement)
            except QueryError as query_error:
                # This is expected to fail if the access policy already exists.
                expected_error = 'Access policy for COLUMN "{column}" already exists on "{table}"'.format(
                    column=column,
                    table=self.table
                )
                if query_error.error_response.message == expected_error:
                    log.debug('An access policy already exists, so this statement was ignored: {0}'.format(statement))
                else:
                    raise

    def run(self):
        """
        Inserts data generated by the copy command into target table.

        Normally you don't want to override this.
        """
        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        self.check_vertica_availability()

        connection = self.output().connect()
        try:
            # create schema and table only if necessary:
            self.create_schema(connection)
            self.create_table(connection)
            self.create_nonaggregate_projections(connection)

            # we should do nothing between initialization and copying
            # that would commit the transaction.
            self.init_copy(connection)

            connection.cursor().execute("SET TIMEZONE TO 'GMT';")

            cursor = connection.cursor()
            self.copy_data_table_from_target(cursor)

            self.analyze_constraints(cursor)
            # mark as complete in same transaction
            self.init_touch(connection)
            self.output().touch(connection)

            # We commit only if both operations completed successfully.
            connection.commit()
            log.debug("Committed transaction.")

            # If we don't do this, the deleted records will significantly impact query performance.
            self.purge_deleted_records(connection)

            # Once we are done with the regular load, go ahead
            # and make sure that aggregate projections are also added.
            # This may be slow, but they would cause commits anyway.
            self.create_aggregate_projections(connection)

            # For some reason we don't fully understand we can't create the access policy before running the COPY.
            # This only works in our crazy schema-swapping mode where we create a fresh schema for loading purposes and
            # then later swap it in.
            self.create_access_policies(connection)

        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            connection.close()

    def check_vertica_availability(self):
        """Call to ensure fast failure if this machine doesn't have the Vertica client library available."""
        if not vertica_client_available:
            raise ImportError('Vertica client library not available')


class IncrementalVerticaCopyTask(VerticaCopyTask):
    """
    A task for copying data into a Vertica database incrementally.

    If overwrite is True, this task only deletes a subset of the table being written to
    and only deletes table_updates row with the same update_id.
    """

    def init_copy(self, connection):
        self.attempted_removal = True
        if self.overwrite:
            # Before changing the current contents table, we have to make sure there
            # are no aggregate projections on it.
            self.drop_aggregate_projections(connection)

            # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
            # commit the currently open transaction before continuing with the copy.
            query = "DELETE FROM {schema}.{table} where {record_filter}".format(
                schema=self.schema,
                table=self.table,
                record_filter=self.record_filter
            )
            log.debug(query)
            connection.cursor().execute(query)

        # vertica-python and its maintainers intentionally avoid supporting open
        # transactions like we do when self.overwrite=True (DELETE a bunch of rows
        # and then COPY some), per https://github.com/uber/vertica-python/issues/56.
        # The DELETE commands in this method will cause the connection to see some
        # messages that will prevent it from trying to copy any data (if the cursor
        # successfully executes the DELETEs), so we flush the message buffer.
        connection.cursor().flush_to_query_ready()

    def init_touch(self, connection):
        if self.overwrite:
            # Clear the appropriate rows from the luigi Vertica marker table
            marker_table = self.output().marker_table  # side-effect: sets self.output_target if it's None
            marker_schema = self.output().marker_schema
            try:
                query = "DELETE FROM {marker_schema}.{marker_table} where update_id='{update_id}';".format(
                    marker_schema=marker_schema,
                    marker_table=marker_table,
                    update_id=self.update_id(),
                )
                log.debug(query)
                connection.cursor().execute(query)
            except vertica_python.errors.Error as err:
                if (type(err) is vertica_python.errors.MissingRelation) or ('Sqlstate: 42V01' in err.args[0]):
                    # If so, then our query error failed because the table doesn't exist.
                    pass
                else:
                    raise
        connection.cursor().flush_to_query_ready()

    @property
    def record_filter(self):
        """
        A string that specifies the data to overwrite, this will be the entire WHERE clause of the generated query.
        """
        raise NotImplementedError


class SchemaManagementTask(VerticaCopyTaskMixin, luigi.Task):
    """
    Base class for running schema management commands on warehouse.

    """

    date = luigi.DateParameter()

    roles = luigi.ListParameter(
        config_path={'section': 'vertica-export', 'name': 'standard_roles'},
    )

    def __init__(self, *args, **kwargs):
        super(SchemaManagementTask, self).__init__(*args, **kwargs)
        self.schema_last = self.schema + '_last'
        self.schema_loading = self.schema + '_loading'
        self.vertica_roles = ','.join(self.roles)

    @property
    def queries(self):
        """
        Provides queries that are needed to run this task.
        """
        raise NotImplementedError

    @property
    def marker_name(self):
        """
        Although we are not writing any data into a table, we still need a marker
        name for the marker table entry so the task can be checked for completeness.
        This should be defined in a derived class.
        """
        raise NotImplementedError

    def requires(self):
        return {
            'credentials': ExternalURL(self.credentials)
        }

    def init_touch(self, connection):
        """
        Clear the relevant rows from the marker table if we are overwriting.
        """
        # This method is the same as appears on vertica_load#L339.
        # However, we do not call cursor.flush_to_query_ready() here as
        # we are not copying any data after this method.
        if self.overwrite:
            # Clear the appropriate rows from the luigi Vertica marker table
            marker_table = self.output().marker_table
            try:
                query = """
                DELETE FROM {marker_schema}.{marker_table} where target_table='{schema}.{target_table}';
                """.format(
                    schema=self.schema,
                    marker_schema=self.marker_schema,
                    marker_table=marker_table,
                    target_table=self.marker_name
                )
                log.debug(query)
                connection.cursor().execute(query)
            except vertica_python.errors.Error as err:
                is_missing_relation = type(err) is vertica_python.errors.MissingRelation
                is_missing_table = 'Sqlstate: 42V01' in err.args[0]
                is_missing_schema = 'Sqlstate: 3F000' in err.args[0]
                if is_missing_relation or is_missing_table or is_missing_schema:
                    # If so, then our query error failed because the schema or table doesn't exist.
                    pass
                else:
                    raise

    def run(self):
        connection = self.output().connect()

        try:
            self.init_touch(connection)
            for query in self.queries:
                log.debug(query)
                connection.cursor().execute(query)

            self.attempted_removal = True
            self.output().touch(connection)
            connection.commit()
        except Exception as exc:
            log.exception("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            connection.close()

    def output(self):
        """
        Returns a VerticaTarget representing the task ran.
        """
        return CredentialFileVerticaTarget(
            credentials_target=self.input()['credentials'],
            table=self.marker_name,
            schema=self.schema,
            update_id=self.update_id(),
            marker_schema=self.marker_schema
        )

    def update_id(self):
        return str(self)
