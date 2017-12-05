"""
Gather data using SQL queries run on MySQL databases.
"""
from __future__ import absolute_import

import csv
import datetime
import json
from contextlib import closing

import luigi
import mysql.connector

from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join


class MysqlSelectTask(luigi.Task):
    """
    A task that reads data out of a MySQL database and writes it to a file in TSV format.  In order to protect the
    database access credentials they are loaded from an external file which can be secured appropriately.  The
    credentials file is expected to be JSON formatted and contain a simple map specifying the host, port, username
    and password.

    Example Credentials File::

        {
            "host": "db.example.com",
            "port": "3306",
            "username": "exampleuser",
            "password": "example password"
        }
    """

    credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    destination = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'destination'},
        description='The directory to write the TSV file to.',
    )
    database = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'database'},
        description='The name of the database to execute the query on.',
    )

    converters = [
        (datetime.date, lambda date: date.strftime('%Y-%m-%d'))
    ]

    @property
    def query(self):
        """
        The SQL query to execute on the database. Results will be streamed in to the TSV output file. Parameters
        can be marked by a question mark (?) and are populated using the tuple returned by the property
        `query_parameters`.

        Returns:
            A string containing the SQL query.
        """
        return "SELECT 1"

    @property
    def query_parameters(self):
        """
        Parameters to pass in to the query. Each element of the tuple will be substituted in to the corresponding
        ? parameter locator in the query. The first element will replace the first question mark, the second will
        replace the second etc.

        Returns:
            A tuple.
        """
        return tuple()

    @property
    def filename(self):
        """
        The name of the output file in the destination directory.

        Returns:
            A string containing the file name.
        """
        raise NotImplementedError  # pragma: no cover

    def requires(self):
        return {
            'credentials': ExternalURL(url=self.credentials)
        }

    def output(self):
        url_with_filename = url_path_join(self.destination, self.filename)
        return get_target_from_url(url_with_filename)

    def run(self):
        with self.connect() as conn:
            """
            this change allows the context manager to work correctly
            """
            try:
                cursor = conn.cursor()

                cursor.execute(self.query, self.query_parameters)

                with self.output().open('w') as output_file:  # pylint: disable=maybe-no-member
                    self.write_results_to_tsv(cursor, output_file)
            finally:
                cursor.close()

    def connect(self):
        """
        Gathers the secure connection parameters from an external file and uses them to establish a connection to the
        MySQL database specified in the secure parameters.

        Returns:
            A context manager that holds open a connection to the database while inside the scope.
        """
        cred = {}
        with self.input()['credentials'].open('r') as credentials_file:
            cred = json.load(credentials_file)

        conn = mysql.connector.connect(
            host=cred['host'],
            port=int(cred['port']),
            user=cred['username'],
            password=cred['password'],
            database=self.database,
        )

        return closing(conn)

    def write_results_to_tsv(self, cursor, output_file):
        """
        Streams results from the cursor to the output file in TSV format.

        Args:
            cursor (mysql.connector.Cursor): A cursor that has already been used to execute an SQL query and now can be used
                to fetch results.
            output_file (file): A file-like object that the records will be written to.
        """

        writer = csv.writer(output_file, delimiter="\t", quoting=csv.QUOTE_NONE)

        while True:
            row = cursor.fetchone()
            if row is None:
                break

            # column values are native python objects, convert them to strings before writing them to the file
            writer.writerow([self.convert(v) for v in row])

    def convert(self, value):
        """
        Converts a python object in to a string that is safe to write out to the TSV file. If an explicit converter
        is not available for a given type then it is converted using `str()`.

        Args:
            value (obj): A python object.

        Returns:
            A string representation of the object or - if `value` is None.
        """
        converted_value = u'-'
        if value is not None:

            def converter(value):
                """Provide default no-op conversion."""
                return value
            for converter_spec in self.converters:
                if isinstance(value, converter_spec[0]):
                    converter = converter_spec[1]
                    break

            converted_value = converter(value)

        return unicode(converted_value).encode('utf-8')


def mysql_datetime(datetime_object):
    """
    Convert a python datetime object to a string that can be parsed by MySQL as a timestamp.

    Args:
        datetime_object (datetime.datetime): The native python representation of the timestamp.

    Returns:
        A string representation of the timestamp in the form "YYYY-MM-DD hh:mm:ss" (e.g. "2014-01-30 22:30:05").
    """
    return datetime_object.strftime('%Y-%m-%d %H:%M:%S')
