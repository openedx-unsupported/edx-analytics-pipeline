
import json
from contextlib import closing, contextmanager

import mysql.connector

from edx.analytics.tasks.util.url import get_target_from_url


# TODO: use a database that is unique to this particular test run to isolate it.
class DatabaseService(object):

    def __init__(self, config, database_name):
        self.credentials_file_url = config['credentials_file_url']
        self.database_name = database_name

    @property
    def credentials(self):
        if not hasattr(self, '_credentials'):
            with get_target_from_url(self.credentials_file_url).open('r') as credentials_file:
                self._credentials = json.load(credentials_file)

        return self._credentials

    @contextmanager
    def cursor(self, explicit_db=True):
        with self.connect(explicit_db=explicit_db) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    yield cur
                except Exception:
                    conn.rollback()
                    raise
                else:
                    conn.commit()

    def execute_sql_file(self, file_path):
        """
        Execute a file containing SQL statements.

        Note that this *does not* use MySQL native mechanisms for parsing *.sql files. Instead it very naively parses
        the statements out of the file itself.

        """
        with self.cursor(explicit_db=True) as cur:
            with open(file_path, 'r') as sql_file:
                for _ignored in cur.execute(sql_file.read(), multi=True):
                    pass

    def connect(self, explicit_db=True):
        """
        Connect to the MySQL server.

        Arguments:
            connect(bool): Use a database for the connection. Set to false to create databases etc.

        """
        kwargs = {
            'host': self.credentials['host'],
            'user': self.credentials['username'],
            'password': self.credentials['password'],
        }
        if explicit_db:
            kwargs['database'] = self.database_name
        return closing(mysql.connector.connect(**kwargs))

    def reset(self):
        """Create a testing database on the MySQL replacing any existing content with an empty database."""
        with self.cursor(explicit_db=False) as cur:
            cur.execute('DROP DATABASE IF EXISTS {0}'.format(self.database_name))
            cur.execute('CREATE DATABASE {0}'.format(self.database_name))
