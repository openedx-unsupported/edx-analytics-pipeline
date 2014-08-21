"""
Import data from external RDBMS databases into Hive.
"""
import datetime
import logging
import textwrap

import luigi

from edx.analytics.tasks.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.hive import ImportIntoHiveTableTask, HivePartition
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)


class DatabaseImportMixin(object):
    """
    Provides general parameters needed for accessing RDBMS databases.

    Parameters:

        destination: The directory to write the output files to.
        credentials: Path to the external access credentials file.
        num_mappers: The number of map tasks to ask Sqoop to use.
        verbose: Print more information while working.  Default is False.
        import_date:  Date to assign to Hive partition.  Default is today's date.

    Example Credentials File::

        {
            "host": "db.example.com",
            "port": "3306",
            "username": "exampleuser",
            "password": "example password"
        }
    """
    destination = luigi.Parameter(
        default_from_config={'section': 'database-import', 'name': 'destination'}
    )
    credentials = luigi.Parameter(
        default_from_config={'section': 'database-import', 'name': 'credentials'}
    )
    import_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())

    num_mappers = luigi.Parameter(default=None, significant=False)
    verbose = luigi.BooleanParameter(default=False, significant=False)


class ImportMysqlToHiveTableTask(DatabaseImportMixin, ImportIntoHiveTableTask):
    """
    Dumps data from an RDBMS table, and imports into Hive.

    Requires override of `table_name` and `columns` properties.
    """

    @property
    def table_location(self):
        return url_path_join(self.destination, self.table_name)

    @property
    def partition(self):
        # Partition date is provided by DatabaseImportMixin.
        return HivePartition('dt', self.import_date.isoformat())

    def requires(self):
        return SqoopImportFromMysql(
            table_name=self.table_name,
            # TODO: We may want to make the explicit passing in of columns optional as it prevents a direct transfer.
            # Make sure delimiters and nulls etc. still work after removal.
            columns=[c[0] for c in self.columns],
            destination=self.partition_location,
            credentials=self.credentials,
            num_mappers=self.num_mappers,
            verbose=self.verbose,
            overwrite=self.overwrite,
            # Hive expects NULL to be represented by the string "\N" in the data. You have to pass in "\\N" to sqoop
            # since it uses that string directly in the generated Java code, so "\\N" actually looks like "\N" to the
            # Java code. In order to get "\\N" onto the command line we have to use another set of escapes to tell the
            # python code to pass through the "\" character.
            null_string='\\\\N',
            # It's unclear why, but this setting prevents us from correctly substituting nulls with \N.
            mysql_delimiters=False,
            # This is a string that is interpreted as an octal number, so it is equivalent to the character Ctrl-A
            # (0x01). This is the default separator for fields in Hive.
            fields_terminated_by='\x01',
            # Replace delimiters with a single space if they appear in the data. This prevents the import of malformed
            # records. Hive does not support escape characters or other reasonable workarounds to this problem.
            delimiter_replacement=' ',
        )


class ImportStudentCourseEnrollmentTask(ImportMysqlToHiveTableTask):
    """Imports course enrollment information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'student_courseenrollment'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('created', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('mode', 'STRING'),
        ]


class ImportAuthUserTask(ImportMysqlToHiveTableTask):

    """Imports course enrollment information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'auth_user'

    @property
    def columns(self):
        # Fields not included are 'password', 'first_name' and 'last_name'.
        # In our LMS, the latter two are always empty.
        return [
            ('id', 'INT'),
            ('username', 'STRING'),
            ('last_login', 'TIMESTAMP'),
            ('date_joined', 'TIMESTAMP'),
            ('is_active', 'BOOLEAN'),
            ('is_superuser', 'BOOLEAN'),
            ('is_staff', 'BOOLEAN'),
            ('email', 'STRING'),
        ]


class ImportAllDatabaseTablesTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of database tables from an external LMS RDBMS."""
    def requires(self):
        kwargs = {
            'destination': self.destination,
            'credentials': self.credentials,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
        }
        yield (
            ImportStudentCourseEnrollmentTask(**kwargs),
            ImportAuthUserTask(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]
