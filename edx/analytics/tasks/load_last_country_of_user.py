"""
Load last country of user from Sqoop and push to Vertica warehouse.
"""
from edx.analytics.tasks.sqoop import SqoopImportTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, HivePartition
import luigi
import luigi.task
from edx.analytics.tasks.vertica_load import VerticaCopyTask


class LoadLastCountryOfUserFromSqoop(SqoopImportTask):

    def connection_url(self, _cred):
        pass

    def output(self):
        """Output is in the form {warehouse_path}/course_catalog_api/catalog/dt={CCYY-MM-DD}/catalog.json.
        For the purpose of applying aggregation to data before pushing into Vertica."""
        date_string = "dt=" + self.run_date.strftime('%Y-%m-%d')  # pylint: disable=no-member
        url_with_filename = url_path_join(self.warehouse_path, "course_catalog", "catalog", date_string,
                                          "catalog.json")
        return get_target_from_url(url_with_filename)

class AggregateDataOfLastCountryOfUser(HiveTableFromQueryTask):
    """Aggregate the last country of user table in Hive."""
    interval = luigi.DateIntervalParameter()
    user_country_output = luigi.Parameter(
        config_path={'section': 'last-country-of-user', 'name': 'user_country_output'}
    )
    n_reduce_tasks = luigi.Parameter()
    destination=luigi.Parameter()
    credentials=luigi.Parameter()
    database=luigi.Parameter()
    table=luigi.Parameter()

    def requires(self):
        """
        This task reads from last_country_of_user, so require that they be
        loaded into Hive (via MySQL loads into Hive or via the pipeline as needed).
        """
        return LoadLastCountryOfUserFromSqoop(destination=self.destination,
                                              credentials=self.credentials,
                                              database=self.database,
                                              tablename=self.tablename)

    @property
    def table(self):
        return 'last_country_of_user'

    @property
    def columns(self):
        return [
            ('user_last_location_country_code', 'VARCHAR(45)'),
            ('country_name', 'VARCHAR(45)')
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.run_date.strftime('%Y-%m-%d'))  # pylint: disable=no-member


    @property
    def insert_query(self):
        return """
                SELECT country_code as user_last_location_country_code, country_name
                FROM last_country_of_user
                GROUP BY country_code
                """

class LoadLastCountryOfUserToWarehouse(VerticaCopyTask):
    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()
    destination=luigi.Parameter()
    credentials=luigi.Parameter()
    database=luigi.Parameter()
    table=luigi.Parameter()

    @property
    def table(self):
        return "last_country_of_user"

    @property
    def columns(self):
        return [
            ('user_last_location_country_code', 'VARCHAR(45)'),
            ('country_name', 'VARCHAR(45)')
        ]

    @property
    def copy_delimiter(self):
        """The delimiter in the data to be copied.  Default is tab (\t). But from sqoop we get comma separated values."""
        return ","

    @property
    def insert_source_task(self):
        return LoadLastCountryOfUserToWarehouse(
            destination=self.destination,
            credentials=self.credentials,
            database=self.database,
            tablename=self.tablename,
            interval=self.interval
        )