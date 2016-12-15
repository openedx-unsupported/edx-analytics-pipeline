"""luigi target for writing data into an HP Vertica database"""
import logging
import json
import luigi

logger = logging.getLogger('luigi-interface')  # pylint: disable-msg=C0103

try:
    import vertica_python
except ImportError:
    logger.warning("Attempted to load Vertica interface tools without the vertica_python package; will crash if \
                   Vertica functionality is used.")


class VerticaTarget(luigi.Target):
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

    def exists(self, connection=None):  # pylint: disable-msg=W0221
        close_connection = False
        if connection is None:
            close_connection = True
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute("""SELECT 1 FROM {marker_schema}.{marker_table}
                WHERE update_id = %s
                LIMIT 1""".format(marker_schema=self.marker_schema, marker_table=self.marker_table),
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

    def __init__(self, credentials_target, schema, table, update_id, read_timeout=None, marker_schema=None):
        with credentials_target.open('r') as credentials_file:
            cred = json.load(credentials_file)
            super(CredentialFileVerticaTarget, self).__init__(
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
            return super(CredentialFileVerticaTarget, self).exists(connection=connection)
        except vertica_python.errors.ProgrammingError:
            return False
