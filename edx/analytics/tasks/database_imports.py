"""
Import data from external RDBMS databases into Hive.
"""
import datetime
import logging
import textwrap

import luigi
from luigi.hive import HiveQueryTask, HivePartitionTarget

from edx.analytics.tasks.sqoop import SqoopImportFromMysql
from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.hive import hive_database_name

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
        config_path={'section': 'database-import', 'name': 'destination'}
    )
    credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'}
    )
    import_date = luigi.DateParameter(default=None)

    num_mappers = luigi.Parameter(default=None, significant=False)
    verbose = luigi.BooleanParameter(default=False, significant=False)

    def __init__(self, *args, **kwargs):
        super(DatabaseImportMixin, self).__init__(*args, **kwargs)

        if not self.import_date:
            self.import_date = datetime.datetime.utcnow().date()


class ImportIntoHiveTableTask(OverwriteOutputMixin, HiveQueryTask):
    """
    Abstract class to import data into a Hive table.

    Requires four properties and a requires() method to be defined.
    """

    def query(self):
        # TODO: Figure out how to clean up old data. This just cleans
        # out old metastore info, and doesn't actually remove the table
        # data.

        # Ensure there is exactly one available partition in the
        # table. Don't keep historical partitions since we don't want
        # to commit to taking snapshots at any regular interval. They
        # will happen when/if they need to happen.  Table snapshots
        # should *not* be used for analyzing trends, instead we should
        # rely on events or database tables that keep historical
        # information.
        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                {col_spec}
            )
            PARTITIONED BY (dt STRING)
            {table_format}
            LOCATION '{location}';
            ALTER TABLE {table_name} ADD PARTITION (dt = '{partition_date}');
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            table_name=self.table_name,
            col_spec=','.join([' '.join(c) for c in self.columns]),
            location=self.table_location,
            table_format=self.table_format,
            partition_date=self.partition_date,
        )

        log.debug('Executing hive query: %s', query)

        # Mark the output as having been removed, even though
        # that doesn't technically happen until the query has been
        # executed (and in particular that the 'DROP TABLE' is executed).
        log.info("Marking existing output as having been removed for task %s", str(self))
        self.attempted_removal = True

        return query

    @property
    def partition(self):
        """Provides name of Hive database table partition."""
        # The Luigi hive code expects partitions to be defined by dictionaries.
        return {'dt': self.partition_date}

    @property
    def partition_location(self):
        """Provides location of Hive database table's partition data."""
        # The actual folder name where the data is stored is expected to be in the format <key>=<value>
        partition_name = '='.join(self.partition.items()[0])
        # Make sure that input path ends with a slash, to indicate a directory.
        # (This is necessary for S3 paths that are output from Hadoop jobs.)
        return url_path_join(self.table_location, partition_name + '/')

    @property
    def table_name(self):
        """Provides name of Hive database table."""
        raise NotImplementedError

    @property
    def table_format(self):
        """Provides format of Hive database table's data."""
        raise NotImplementedError

    @property
    def table_location(self):
        """Provides root location of Hive database table's data."""
        raise NotImplementedError

    @property
    def partition_date(self):
        """Provides value to use in constructing the partition name of Hive database table."""
        raise NotImplementedError

    @property
    def columns(self):
        """
        Provides definition of columns in Hive.

        This should define a list of (name, definition) tuples, where
        the definition defines the Hive type to use. For example,
        ('first_name', 'STRING').

        """
        raise NotImplementedError

    def output(self):
        return HivePartitionTarget(
            self.table_name, self.partition, database=hive_database_name(), fail_missing_table=False
        )


class ImportMysqlToHiveTableTask(DatabaseImportMixin, ImportIntoHiveTableTask):
    """
    Dumps data from an RDBMS table, and imports into Hive.

    Requires override of `table_name` and `columns` properties.
    """

    @property
    def table_location(self):
        return url_path_join(self.destination, self.table_name)

    @property
    def table_format(self):
        # Use default of hive built-in format.
        return ""

    @property
    def partition_date(self):
        # Partition date is provided by DatabaseImportMixin.
        return self.import_date.isoformat()

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

    """Imports user information from an external LMS DB to a destination directory."""

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


class ImportAuthUserProfileTask(ImportMysqlToHiveTableTask):

    """Imports user demographic information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'auth_userprofile'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('name', 'STRING'),
            ('gender', 'STRING'),
            ('year_of_birth', 'INT'),
            ('level_of_education', 'STRING'),
        ]


class ImportCourseUserGroupTask(ImportMysqlToHiveTableTask):
    """Imports course cohort information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'course_groups_courseusergroup'

    @property
    def columns(self):
        return [
            ('id', 'INT'),
            ('name', 'STRING'),
            ('course_id', 'STRING'),
            ('group_type', 'STRING'),
        ]


class ImportCourseUserGroupUsersTask(ImportMysqlToHiveTableTask):
    """Imports user cohort information from an external LMS DB to a destination directory."""

    @property
    def table_name(self):
        return 'course_groups_courseusergroup_users'

    @property
    def columns(self):
        return [
            ('courseusergroup_id', 'INT'),
            ('user_id', 'INT'),
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
            ImportAuthUserProfileTask(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]
