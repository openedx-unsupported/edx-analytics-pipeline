"""luigi target for writing data into an HP Vertica database"""
import logging
import json
import luigi
import time

from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound

logger = logging.getLogger('luigi-interface')  # pylint: disable=invalid-name


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)


class BigQueryTarget(luigi.Target):
    def __init__(self, credentials_target, dataset_id, table, update_id):
        self.dataset_id = dataset_id
        self.table = table
        self.update_id = update_id
        with credentials_target.open('r') as credentials_file:
            json_creds = json.load(credentials_file)
            self.project_id = json_creds['project_id']
            credentials = service_account.Credentials.from_service_account_info(json_creds)
            self.client = bigquery.Client(credentials=credentials, project=self.project_id)

    def touch(self):
        self.create_marker_table()
        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table('table_updates')
        table.reload() # Load the schema

        # Use a tempfile for loading data into table_updates
        # We deliberately don't use table.insert_data as it we cannot use delete on
        # a bigquery table with streaming inserts.
        tmp = tempfile.NamedTemporaryFile(bufsize=0)
        table_update_row  = (self.update_id, "{dataset}.{table}".format(dataset=self.dataset_id, table=self.table))
        tmp.write(','.join(table_update_row))

        tmp.seek(0)
        job = table.upload_from_file(tmp, source_format='text/csv')

        try:
            wait_for_job(job)
        finally:
            tmp.close()

    def create_marker_table(self):
        marker_table_schema = [
            bigquery.SchemaField('update_id', 'STRING'),
            bigquery.SchemaField('target_table', 'STRING'),
        ]

        dataset = self.client.dataset(self.dataset_id)
        table = dataset.table('table_updates', marker_table_schema)
        if not table.exists():
            table.create()

    def exists(self):
        query_string = "SELECT 1 FROM {dataset}.table_updates where update_id = '{update_id}'".format(
            dataset=self.dataset_id,
            update_id=self.update_id,
        )
        log.debug(query_string)

        query = self.client.run_sync_query(query_string)

        try:
            query.run()
        except NotFound:
            return False

        return len(query.rows) == 1


class BigQueryTarget(luigi.Target):
    """
    Target for a resource in HP Vertica
    """
    marker_table = 'table_updates'

    def __init__(self, host, user, password, schema, table, update_id, read_timeout=None, marker_schema=None):
        """
        Initializes a VerticaTarget instance.

        :param host: Vertica server address.  Possibly a host:port string.
        :type host: str
        :param user: database user.
        :type user: str
        :param password: password for the specified user.
        :type password: str
        :param schema: the schema being written to.
        :type schema: str
        :param table: the table within schema being written to.
        :type table: str
        :param update_id: an identifier for this data set.
        :type update_id: str
        """
        # Make sure we can connect to Vertica.
        self.check_vertica_availability()

        if ':' in host:
            self.host, self.port = host.split(':')
            self.port = int(self.port)
        else:
            self.host = host
            self.port = 5433
        self.user = user
        self.password = password
        self.schema = schema
        self.table = table
        self.update_id = update_id

        # Default to using the schema data is being inserted into as the schema for the marker table
        # if no value is provided for marker_schema.
        if marker_schema:
            self.marker_schema = marker_schema
        else:
            self.marker_schema = schema
        self.read_timeout = read_timeout

    def touch(self, connection=None):
        """
        Mark this update as complete.
        IMPORTANT, If the marker table doesn't exist,
        the connection transaction will be aborted and the connection reset.
        Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        connection.cursor().execute(
            """INSERT INTO {marker_schema}.{marker_table} (update_id, target_table)
               VALUES (%s, %s)""".format(marker_schema=self.marker_schema, marker_table=self.marker_table),
            (self.update_id, "{schema}.{table}".format(schema=self.schema, table=self.table))
        )

        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):  # pylint: disable=arguments-differ
        close_connection = False
        if connection is None:
            close_connection = True
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(
                """SELECT 1 FROM {marker_schema}.{marker_table}
                WHERE update_id = %s
                LIMIT 1""".format(
                    marker_schema=self.marker_schema, marker_table=self.marker_table),
                (self.update_id,)
            )
            row = cursor.fetchone()
        except vertica_python.errors.Error as err:
            if (type(err) is vertica_python.errors.MissingRelation) or ('Sqlstate: 42V01' in err.args[0]):
                # If so, then our query error failed because the table doesn't exist.
                row = None
            else:
                raise
        finally:
            if close_connection:
                connection.close()
        return row is not None

    def connect(self, autocommit=False):
        """
        Creates a connection to a Vertica database using the supplied credentials.

        :param autocommit: whether the connection should automatically commit.
        :type autocmommit: bool
        """

        # vertica-python 0.5.0 changes the code for connecting to databases to use kwargs instead of a dictionary.
        # The 'database' parameter is included for DBAPI reasons and does not actually affect the session.
        connection = vertica_python.connect(user=self.user, password=self.password, host=self.host, port=self.port,
                                            database="", autocommit=autocommit, read_timeout=self.read_timeout)
        return connection

    def create_marker_table(self):
        """
        Create marker table if it doesn't exist.
        Using a separate connection since the transaction might have to be reset.
        """
        connection = self.connect(autocommit=True)
        self.create_marker_schema(connection)
        cursor = connection.cursor()
        try:
            cursor.execute(
                """ CREATE TABLE {marker_schema}.{marker_table} (
                        id            AUTO_INCREMENT,
                        update_id     VARCHAR(4096)  NOT NULL,
                        target_table  VARCHAR(128),
                        inserted      TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (update_id, id)
                    )
                """.format(marker_schema=self.marker_schema, marker_table=self.marker_table)
            )
        except vertica_python.errors.QueryError as err:
            if 'Sqlstate: 42710' in err.args[0]:  # This Sqlstate will appear if the marker table already exists.
                pass
            else:
                raise
        connection.close()

    def create_marker_schema(self, connection):
        """
        Create the marker_schema if it does not exist.
        """
        query = "CREATE SCHEMA IF NOT EXISTS {marker_schema}".format(marker_schema=self.marker_schema)
        connection.cursor().execute(query)

    def check_vertica_availability(self):
        """Call to ensure fast failure if this machine doesn't have the Vertica client library available."""
        if not vertica_client_available:
            raise ImportError('Vertica client library not available')


class CredentialFileBigQueryTarget(BigQueryTarget):
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

    def __init__(self, credentials_target, schema, table, update_id, read_timeout=None, marker_schema=None):
        #TODO AZ I don't think this is needed, but it should be strongly considered as it fits in our security model
        with credentials_target.open('r') as credentials_file:
            cred = json.load(credentials_file)
            super(CredentialFileBigQueryTarget, self).__init__(
                # Annoying, but the port must be passed in with the host string...
                host="{host}:{port}".format(host=cred.get('host'), port=cred.get('port', 5433)),
                user=cred.get('username'),
                password=cred.get('password'),
                schema=schema,
                table=table,
                update_id=update_id,
                read_timeout=read_timeout,
                marker_schema=marker_schema,
            )

    def exists(self, connection=None):
        # The parent class fails if the database does not exist. This override tolerates that error.
        try:
            return super(CredentialFileBigQueryTarget, self).exists(connection=connection)
        except vertica_python.errors.ProgrammingError:
            return False
