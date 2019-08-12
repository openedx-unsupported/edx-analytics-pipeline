"""
Support for loading data into a Mysql database.
"""
import json
import logging
import traceback
from itertools import chain

import luigi.configuration
from luigi.contrib.mysqldb import MySqlTarget

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)

try:
    import mysql.connector
    from mysql.connector.errors import ProgrammingError
    from mysql.connector import errorcode
    mysql_client_available = True
except ImportError:
    log.warn('Unable to import mysql client libraries')
    # On hadoop slave nodes we don't have mysql client libraries installed so it is pointless to ship this package to
    # them, instead just fail noisily if we attempt to use these libraries.
    mysql_client_available = False


class MysqlInsertTaskMixin(OverwriteOutputMixin):
    """
    Parameters for inserting a data set into RDBMS.

    """
    database = luigi.Parameter(
        config_path={'section': 'database-export', 'name': 'database'},
        description='The name of the database to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'database-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    insert_chunk_size = luigi.IntParameter(
        default=100,
        significant=False,
        description='The number of rows to insert at a time.',
    )
    use_temp_table_for_overwrite = luigi.BoolParameter(
        default=False,
        significant=False,
        description='Whether to use a temp table for overwriting mysql data followed by a rename.'
    )


class MysqlInsertTask(MysqlInsertTaskMixin, luigi.Task):
    """
    A task for inserting a data set into RDBMS.

    """
    required_tasks = None
    output_target = None
    allow_empty_insert = False

    def requires(self):
        if self.required_tasks is None:
            self.required_tasks = {
                'credentials': ExternalURL(url=self.credentials),
            }
            if not self.insert_source_task_dynamically:
                self.required_tasks['insert_source'] = self.insert_source_task

        return self.required_tasks

    @property
    def insert_source_task(self):
        """Defines task that provides source of data for insertion."""
        raise NotImplementedError

    @property
    def insert_source_task_dynamically(self):
        """Declare if task that provides source of data for insertion should be a dynamic dependency."""
        return False

    @property
    def table(self):
        """Provides name of database table."""
        raise NotImplementedError

    @property
    def columns(self):
        """
        Provides definition of columns.

        If only writing to existing tables, then columns() need only provide a list of names.

        If also needing to create the table, then columns() should define a list of
        (name, definition) tuples. For example, ('first_name', 'VARCHAR(255)').
        """
        raise NotImplementedError

    @property
    def auto_primary_key(self):
        """Tuple defining name and definition of auto-incrementing primary key, or None."""
        return ('id', 'BIGINT(20) NOT NULL AUTO_INCREMENT')

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return [('created', 'TIMESTAMP DEFAULT NOW()')]

    @property
    def indexes(self):
        """List of tuples defining the names of the columns to include in each index."""
        return []

    @property
    def keys(self):
        """List of tuples defining other keys to include in the table definition."""
        return []

    def create_table(self, connection):
        """
        Override to provide code for creating the target table, if not existing.

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
        columns = []
        if self.auto_primary_key is not None:
            columns.append(self.auto_primary_key)
        columns.extend(self.columns)
        columns.extend(self.default_columns)
        if self.auto_primary_key is not None:
            columns.append(("PRIMARY KEY", "({name})".format(name=self.auto_primary_key[0])))
        for indexed_cols in self.indexes:
            columns.append(("INDEX", "({cols})".format(cols=','.join(indexed_cols))))
        for key in self.keys:
            columns.append((key[0], "({cols})".format(cols=','.join(key[1]))))

        coldefs = ','.join(
            '{name} {definition}'.format(name=name, definition=definition) for name, definition in columns
        )
        query = "CREATE TABLE IF NOT EXISTS {table} ({coldefs})".format(
            table=self.table, coldefs=coldefs
        )
        log.debug(query)
        connection.cursor().execute(query)

    def create_database(self):
        """Create the database if it doesn't exist yet."""

        output_target = self.output()

        # The default behavior of MysqlTarget is to connect to a specific database, which will fail since the database
        # doesn't exist yet, so we make our own connection here that is not attached to a specific database.
        connection = mysql.connector.connect(
            user=output_target.user,
            password=output_target.password,
            host=output_target.host,
            port=output_target.port,
            autocommit=True  # These operations autocommit anyway.
        )
        try:
            cursor = connection.cursor()
            query = "CREATE DATABASE IF NOT EXISTS {db}".format(db=self.database)
            log.debug(query)
            cursor.execute(query)
        finally:
            connection.close()

    def rows(self):
        """Return/yield tuples or lists corresponding to each row to be inserted """
        try:
            if self.insert_source_task is not None:
                input_target = self.insert_source_task.output()
                with input_target.open('r') as fobj:
                    for line in fobj:
                        yield line.strip('\n').split('\t')
        except RuntimeError:
            # While calling finish on an input target, Luigi throws a RuntimeError exception if the subprocess command
            # to read the input returns a non-zero return code. As all of the data's been read already, we choose to ignore
            # this exception.
            traceback_str = traceback.format_exc()
            if "self._finish()" in traceback_str:
                log.debug("Luigi raised RuntimeError while calling _finish on input target.")
            else:
                raise

    def update_id(self):
        """This update id will be a unique identifier for this insert on this table."""
        # The hash of the task is made by hashing the task_id, which
        # in turn combines the task name and the significant
        # arguments.  Using the task_id itself would be more readable
        # and debuggable, and is used with Postgres.  But because the
        # column is indexed, and because Mysql has a limit of 767
        # characters on a key, we need to hash the task_id to be sure
        # to fit.
        return str(hash(self))

    def output(self):
        """
        Returns a MysqlTarget representing the inserted dataset.

        Normally you don't override this.
        """
        if self.output_target is None:
            self.output_target = CredentialFileMysqlTarget(
                credentials_target=self.input()['credentials'],
                database_name=self.database,
                table=self.table,
                update_id=self.update_id()
            )

        return self.output_target

    def init_copy(self, connection):
        """
        Override to perform custom queries.

        Any code here will be formed in the same transaction as the
        main copy, just prior to copying data. Example use cases
        include truncating the table or removing all data older than X
        in the database to keep a rolling window of data available in
        the table.
        """
        # clear table contents
        self.attempted_removal = True
        if self.overwrite:
            # first clear the appropriate rows from the luigi mysql marker table
            marker_table = self.output().marker_table  # side-effect: sets self.output_target if it's None
            try:
                query = "DELETE FROM {marker_table} where `target_table`='{target_table}'".format(
                    marker_table=marker_table,
                    target_table=self.table,
                )
                log.debug(query)
                connection.cursor().execute(query)
            except mysql.connector.Error as excp:  # handle the case where the marker_table has yet to be created
                if excp.errno == errorcode.ER_NO_SUCH_TABLE:
                    pass
                else:
                    raise

            if not self.use_temp_table_for_overwrite:
                # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
                # commit the currently open transaction before continuing with the copy.
                query = "DELETE FROM {table}".format(table=self.table)
                log.debug(query)
                connection.cursor().execute(query)

    def _execute_insert_query(self, cursor, value_list, column_names):
        """
        Constructs and executes the insert query.

        Parameters:
            cursor - database cursor to use for execution
            value_list - a list of tuples to insert.  The number of tuples
                corresponds to the number of rows, and each tuple should have
                an element for each column.
            column_names - a single string holding names of columns, joined by commas.

        Example:

            column_names = 'col_1, col_2, col_3'
            value_list = [('foo', 0, 0), ('bar', 1, 1), ('baz', 2, 2)]

            results in:

            INSERT INTO table_name (col_1, col_2, col_3)
                VALUES (%s, %s, %s), (%s, %s, %s), (%s, %s, %s)
        """

        num_cols = len(self.columns)
        num_rows = len(value_list)

        # Check data squareness.  There should be no rows with missing or extra columns.
        for elem in value_list:
            if len(elem) != num_cols:
                raise Exception("Misaligned data in mysql_load: "
                                "row '{row}' does not match columns '{columns}'".format(
                                    row=elem, columns=column_names
                                ))

        # The "%s" placeholder is used by the mysql-connector library
        # to execute the prepared statement, it is not used with a
        # traditional python "%" operator.
        parameters = "(" + ",".join(["%s"] * num_cols) + ")"
        all_parameters = ",".join([parameters] * num_rows)

        table_name = self.table
        if self.overwrite and self.use_temp_table_for_overwrite:
            table_name = self.table + '_temp'

        query = "INSERT INTO {table} ({column_names}) VALUES {values}".format(
            table=table_name, column_names=column_names, values=all_parameters
        )
        cursor.execute(query, list(chain.from_iterable(value_list)))
        log.debug("Wrote %d rows to table %s", num_rows, self.table)

    def insert_rows(self, cursor):
        """Inserts row values from source into database table."""
        if isinstance(self.columns[0], basestring):
            column_names = ','.join([name for name in self.columns])
        elif len(self.columns[0]) == 2:
            column_names = ','.join([name for name, _type in self.columns])
        else:
            raise Exception('columns must consist of column strings or '
                            '(column string, type string) tuples (was %r ...)'
                            % (self.columns[0],))

        value_list = []
        row_count = 0
        for row_count, row in enumerate(self.rows(), start=1):
            entry = tuple([coerce_for_mysql_connect(elem) for elem in row])
            value_list.append(entry)
            if row_count % self.insert_chunk_size == 0:
                self._execute_insert_query(cursor, value_list, column_names)
                value_list = []

        if self.overwrite and not self.allow_empty_insert and row_count == 0:
            raise Exception('Cannot overwrite a table with an empty result set.')

        if len(value_list) > 0:
            self._execute_insert_query(cursor, value_list, column_names)

    def run(self):
        """
        Inserts data generated by rows() into target table.

        Normally you don't want to override this.
        """
        # Use dynamic dependencies here to make sure that the tasks on
        # which this depends have been run.
        if self.insert_source_task_dynamically and self.insert_source_task is not None:
            log.debug('Yielding dependency dynamically at runtime for %s: %s', self, self.insert_source_task)
            yield self.insert_source_task
        else:
            yield []

        if not (self.table and self.columns):
            raise Exception("table and columns need to be specified")

        self.check_mysql_availability()

        # create databases using a separate connection which is not database specific
        self.create_database()

        connection = self.output().connect()
        try:
            # create table only if necessary:
            self.create_table(connection)

            if self.overwrite and self.use_temp_table_for_overwrite:
                self.drop_temp_tables(connection)
                query = "CREATE TABLE {table_temp} LIKE {table}".format(
                    table_temp=self.table + '_temp', table=self.table
                )
                log.debug(query)
                connection.cursor().execute(query)
            # This prevents gap locks when updating the marker table, enabling us to insert and update records in that
            # table with impunity from other sessions.
            connection.cursor().execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

            self.init_copy(connection)
            cursor = connection.cursor()
            self.insert_rows(cursor)

            # mark as complete in same transaction
            self.output().touch(connection)

            if self.overwrite and self.use_temp_table_for_overwrite:
                # Note that this would cause an implicit commit.
                query = "RENAME TABLE {table} to {table_old}, {table_temp} to {table}".format(
                    table=self.table, table_old=self.table + '_old', table_temp=self.table + '_temp'
                )
                log.debug(query)
                connection.cursor().execute(query)
            else:
                # commit is only necessary if we're not getting an implicit commit from the rename
                connection.commit()
        except Exception as e:
            log.error(str(e))
            if connection.is_connected():
                connection.rollback()
            raise
        finally:
            # Some errors will close the connection
            if not connection.is_connected():
                connection = self.output().connect()

            try:
                # Try to drop the tables here in case the job hits a failure.
                self.drop_temp_tables(connection)
            except mysql.connector.Error:
                # Eat this exception so that the original culprit gets raised.
                # If something earlier failed we can expect that these tables
                # may not exist, but at least log the failure.
                log.error("Unable to drop temp tables, might not exist?")

            connection.close()

    def check_mysql_availability(self):
        if not mysql_client_available:
            raise ImportError('mysql client library not available')

    def drop_temp_tables(self, connection):
        for table in [self.table + '_old', self.table + '_temp']:
            query = "DROP TABLE IF EXISTS {table}".format(
                table=table
            )
            log.debug(query)
            connection.cursor().execute(query)


# Helper methods
def coerce_for_mysql_connect(input):
    """
    Given an input which could be any python type, try to coerce it to something acceptable to mysql-connect

    The most important case is the conversion of string 'None' to actual None, and also the conversion
    of str to decoded utf-8 unicode
    """
    if not isinstance(input, basestring):
        return input
    # Hive indicates a null value with the string "\N"
    # We represent an infinite value with the string "inf", MySQL has no such representation so we use NULL
    if input in ('None', '\\N', 'inf', '-inf'):
        return None
    if isinstance(input, str):
        return input.decode('utf-8')
    return input


def get_mysql_query_results(credentials, database, query):
    """
    Executes a mysql query on the provided database and returns the results.
    """

    credentials_target = ExternalURL(url=credentials).output()
    cred = None
    with credentials_target.open('r') as credentials_file:
        cred = json.load(credentials_file)

    connection = mysql.connector.connect(user=cred.get('username'),
                                         password=cred.get('password'),
                                         host=cred.get('host'),
                                         port=cred.get('port'),
                                         database=database)

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
    finally:
        connection.close()

    return results


class CredentialFileMysqlTarget(MySqlTarget):
    """
    Represents a table in MySQL, is complete when the update_id is the same as a previous successful execution.

    Arguments:
        credentials_target (luigi.Target): A target that can be read to retrieve the hostname, port and user credentials
            that will be used to connect to the database.
        database_name (str): The name of the database that the table exists in. Note this database need not exist.
        table (str): The name of the table in the database that is being modified.
        update_id (str): A unique identifier for this update to the table. Subsequent updates with identical update_id
            values will not be executed.

    """

    def __init__(self, credentials_target, database_name, table, update_id):
        with credentials_target.open('r') as credentials_file:
            cred = json.load(credentials_file)
            return super(CredentialFileMysqlTarget, self).__init__(
                # Annoying, but the port must be passed in with the host string...
                host="{host}:{port}".format(host=cred.get('host'), port=cred.get('port', 3306)),
                database=database_name,
                user=cred.get('username'),
                password=cred.get('password'),
                table=table,
                update_id=update_id
            )

    def exists(self, connection=None):
        # The parent class fails if the database does not exist. This override tolerates that error.
        try:
            return super(CredentialFileMysqlTarget, self).exists(connection=connection)
        except ProgrammingError:
            return False

    def create_marker_table(self):
        """
        Override the default luigi logic here since we also need an index on target_table to prevent InnoDB from locking
        every row in the table when we execute a DELETE FROM WHERE target_table="foo". By default it will lock any row
        that is scanned during the preparation for the DELETE, so we need to have an index on target_table to ensure
        that other workflows that are being committed can also update the marker table while this transaction is being
        committed.
        """
        connection = self.connect(autocommit=True)
        cursor = connection.cursor()
        try:
            cursor.execute(
                """ CREATE TABLE IF NOT EXISTS {marker_table} (
                        id            BIGINT(20)    NOT NULL AUTO_INCREMENT,
                        update_id     VARCHAR(128)  NOT NULL,
                        target_table  VARCHAR(128),
                        inserted      TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (update_id),
                        KEY id (id),
                        INDEX target_table (target_table)
                    )
                """
                .format(marker_table=self.marker_table)
            )
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                pass
            else:
                raise
        connection.close()


class IncrementalMysqlInsertTask(MysqlInsertTask):
    """
    A MySQL table that is mostly appended to, but occasionally has parts of it overwritten.

    When overwriting, the task is responsible for populating some records that need to be easy to identify. There should
    be a one-to-one relationship between a row and the task that was used to write it. It should be straightforward to
    construct a where clause that selects all of the rows generated by this task.
    """

    def __init__(self, *args, **kwargs):
        super(IncrementalMysqlInsertTask, self).__init__(*args, **kwargs)
        if self.use_temp_table_for_overwrite:
            raise Exception("Cannot use IncrementalMysqlInsertTask with use_temp_table_for_overwrite set to True")

    def init_copy(self, connection):
        # clear only the data for this date!

        self.attempted_removal = True
        if self.overwrite:
            # first clear the appropriate rows from the luigi mysql marker table
            # side-effect: sets self.output_target if it's None
            marker_table = self.output().marker_table  # pylint: disable=no-member
            try:
                query = "DELETE FROM {marker_table} where `update_id`='{update_id}'".format(
                    marker_table=marker_table,
                    update_id=self.update_id(),
                )
                log.debug(query)
                connection.cursor().execute(query)
            except mysql.connector.Error as excp:  # handle the case where the marker_table has yet to be created
                if excp.errno == errorcode.ER_NO_SUCH_TABLE:
                    pass
                else:
                    raise

            # Use "DELETE" instead of TRUNCATE since TRUNCATE forces an implicit commit before it executes which would
            # commit the currently open transaction before continuing with the copy.
            query = "DELETE FROM {table} WHERE {record_filter}".format(
                table=self.table,
                record_filter=self.record_filter
            )
            log.debug(query)
            connection.cursor().execute(query)

    @property
    def record_filter(self):
        """
        A string that specifies the data to overwrite, this will be the entire WHERE clause of the generated query.
        """
        raise NotImplementedError
