"""Service for connecting acceptance tests to Vertica."""
import json
from contextlib import closing, contextmanager

import vertica_python

from edx.analytics.tasks.util.url import get_target_from_url


class VerticaService(object):
    """Service object to be used as a member of a class to enable that class to write to and read from Vertica."""

    def __init__(self, config, schema_name):
        self.vertica_creds_url = config.get('vertica_creds_url')
        if not self.vertica_creds_url:
            self.disabled = True
        else:
            self.disabled = False
        self.schema_name = schema_name

    @property
    def credentials(self):
        """The credentials for connecting to the database, read from a URL."""
        if not hasattr(self, '_credentials'):
            with get_target_from_url(self.vertica_creds_url).open('r') as credentials_file:
                self._credentials = json.load(credentials_file)

        return self._credentials

    @contextmanager
    def cursor(self):
        """A cursor for the database connection, as a context manager that can be opened and closed."""
        with self.connect() as conn:
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

        Note that this *does not* use Vertica native mechanisms for parsing *.sql files. Instead it very naively parses
        the statements out of the file itself.

        """
        with self.cursor() as cur:
            with open(file_path, 'r') as sql_file:
                for line in sql_file:
                    if line.startswith('--') or len(line.strip()) == 0:
                        continue

                    cur.execute(line)

    def connect(self):
        """
        Connect to the Vertica server.
        """
        return vertica_python.connect(user=self.credentials.get('username'), password=self.credentials.get('password'),
                                      database='', host=self.credentials.get('host'))

    def reset(self):
        """Create a testing schema on the Vertica database replacing any existing content with an empty database."""
        if self.disabled:
            return

        with self.cursor() as cur:
            # In vertica, an epoch ( a logical time stamp for the data in Vertica ) is assigned to every row,
            # which gets updated whenever data is committed via DML operation.
            # Vertica advances the AHM at an interval of 5mins, but when there is a node down in vertica cluster,
            # AHM does not advance. MAKE_AHM_NOW() advances the epoch to the greatest allowable value,
            # and lets you drop any projections that existed before the issue occurred,
            # all history is lost and no historical queries cannot be made before current epoch.
            reset_query = 'DROP SCHEMA IF EXISTS {0} CASCADE; CREATE SCHEMA {0}; SELECT MAKE_AHM_NOW();'.format(self.schema_name)
            cur.execute(reset_query)
