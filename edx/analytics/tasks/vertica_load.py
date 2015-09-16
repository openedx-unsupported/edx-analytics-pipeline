"""
Support for loading data into an HP Vertica database.
"""
import json
import logging

import luigi
import luigi.configuration
from edx.analytics.tasks.url import ExternalURL

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.vertica_target import VerticaTarget

log = logging.getLogger(__name__)

try:
    import vertica_python
    vertica_client_available = True  # pylint: disable-msg=C0103
except ImportError:
    log.warn('Unable to import Vertica client libraries')
    # On hadoop slave nodes we don't have Vertica client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    vertica_client_available = False  # pylint: disable-msg=C0103


class VerticaCopyTaskMixin(OverwriteOutputMixin):
    """
    Parameters for copying a database into Vertica.

        credentials: Path to the external access credentials file.
        schema:  The schema to which to write.
        insert_chunk_size:  The number of rows to insert at a time.
    """
    schema = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'schema'}
    )
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'}
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
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return [('created', 'TIMESTAMP DEFAULT NOW()')]

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

        query = "CREATE TABLE IF NOT EXISTS {schema}.{table} ({coldefs}{foreign_key_defs})".format(
            schema=self.schema, table=self.table, coldefs=coldefs, foreign_key_defs=foreign_key_defs
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
                update_id=self.update_id()
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
            try:
                query = "DELETE FROM {schema}.{marker_table} where target_table='{schema}.{target_table}';".format(
                    schema=self.schema,
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

        with self.input()['insert_source'].open('r') as insert_source_file:
            log.debug("Running stream copy from source file")
            cursor.copy(
                "COPY {schema}.{table} ({cols}) FROM STDIN DELIMITER AS {delim} NULL AS {null} DIRECT ABORT ON ERROR NO COMMIT;".format(
                    schema=self.schema,
                    table=self.table,
                    cols=column_names,
                    delim=self.copy_delimiter,
                    null=self.copy_null_sequence
                ),
                insert_source_file
            )

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

            self.init_copy(connection)
            cursor = connection.cursor()
            self.copy_data_table_from_target(cursor)

            # mark as complete in same transaction
            self.init_touch(connection)
            self.output().touch(connection)

            # We commit only if both operations completed successfully.
            connection.commit()
            log.debug("Committed transaction.")
        except Exception as exc:
            log.debug("Rolled back the transaction; exception raised: %s", str(exc))
            connection.rollback()
            raise
        finally:
            connection.close()

    def check_vertica_availability(self):
        """Call to ensure fast failure if this machine doesn't have the Vertica client library available."""
        if not vertica_client_available:
            raise ImportError('Vertica client library not available')


class CredentialFileVerticaTarget(VerticaTarget):
    """
    Represents a table in Vertica, is complete when the update_id is the same as a previous successful execution.

    Arguments:

        credentials_target (luigi.Target): A target that can be read to retrieve the hostname, port and user credentials
            that will be used to connect to the database.
        database_name (str): The name of the database that the table exists in. Note this database need not exist.
        schema (str): The name of the schema in which the table being modified lies.
        table (str): The name of the table in the schema that is being modified.
        update_id (str): A unique identifier for this update to the table. Subsequent updates with identical update_id
            values will not be executed.
    """

    def __init__(self, credentials_target, schema, table, update_id):
        with credentials_target.open('r') as credentials_file:
            cred = json.load(credentials_file)
            super(CredentialFileVerticaTarget, self).__init__(
                # Annoying, but the port must be passed in with the host string...
                host="{host}:{port}".format(host=cred.get('host'), port=cred.get('port', 5433)),
                user=cred.get('username'),
                password=cred.get('password'),
                schema=schema,
                table=table,
                update_id=update_id
            )

    def exists(self, connection=None):
        # The parent class fails if the database does not exist. This override tolerates that error.
        try:
            return super(CredentialFileVerticaTarget, self).exists(connection=connection)
        except vertica_python.errors.ProgrammingError:
            return False
