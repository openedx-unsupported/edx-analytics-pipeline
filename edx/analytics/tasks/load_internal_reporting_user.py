"""
Loads the user table into the warehouse through the pipeline via Hive.

On the roadmap is to write a task that runs validation queries on the aggregated Hive data pre-load.
"""
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.location_per_course import ExternalLastCountryOfUserToHiveTask
from edx.analytics.tasks.database_imports import ImportAuthUserProfileTask, ImportAuthUserTask
import luigi
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, WarehouseMixin, HivePartition


class AggregateInternalReportingUserTableHive(HiveTableFromQueryTask):
    """Aggregate the internal reporting user table in Hive."""
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()

    def requires(self):
        """
        This task reads from auth_user, auth_user_profile, and last_country_of_user, so require that they be
        loaded into Hive (via MySQL loads into Hive or via the pipeline as needed).
        """
        return [ImportAuthUserTask(overwrite=self.overwrite, destination=self.warehouse_path),
                ImportAuthUserProfileTask(overwrite=self.overwrite, destination=self.warehouse_path),
                ExternalLastCountryOfUserToHiveTask(date=self.date)]

    @property
    def table(self):
        return 'internal_reporting_user'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('year_of_birth', 'INT'),
            ('level_of_education', 'STRING'),
            ('gender', 'STRING'),
            ('email', 'STRING'),
            ('username', 'STRING'),
            ('date_joined', 'TIMESTAMP'),
            ('last_country', 'STRING')
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
              au.id
            , aup.year_of_birth
            , aup.level_of_education
            , aup.gender
            , au.email
            , au.username
            , au.date_joined
            , COALESCE(lc.country_code, 'UNKNOWN')
            FROM auth_user au
            JOIN auth_userprofile aup ON au.id = aup.user_id
            LEFT OUTER JOIN last_country_of_user lc ON au.username = lc.username
            SORT BY au.username ASC
            """


class LoadInternalReportingUserToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    Loads the user table from Hive into the Vertica data warehouse.

    """
    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter(
        description='Number of reduce tasks',
    )

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return (
            # Get the location of the Hive table, so it can be opened and read.
            AggregateInternalReportingUserTableHive(
                n_reduce_tasks=self.n_reduce_tasks,
                date=self.date,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
        )

    @property
    def table(self):
        return 'd_user'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def auto_primary_key(self):
        """No automatic primary key here; user's id is enough."""
        return None

    @property
    def columns(self):
        """
        Note that the official schema lists user_account_activation_time TIMESTAMP as well, but we don't currently
        have the ability to pull that in (in fact, it shows up as null in the production.d_course table as well),
        so it is left out here.
        """
        return [
            ('user_id', 'INTEGER'),
            ('user_year_of_birth', 'INTEGER'),
            ('user_level_of_education', 'VARCHAR(200)'),
            ('user_gender', 'VARCHAR(45)'),
            ('user_email', 'VARCHAR(100)'),
            ('user_username', 'VARCHAR(45)'),
            ('user_account_creation_time', 'TIMESTAMP'),
            ('user_last_location_country_code', 'VARCHAR(45)')
        ]
