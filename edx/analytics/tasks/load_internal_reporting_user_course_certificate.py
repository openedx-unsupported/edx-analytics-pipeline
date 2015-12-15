"""
Load data from Sqoop and push it into Vertica warehouse.
"""

from edx.analytics.tasks.sqoop import SqoopImportTask
from edx.analytics.tasks.vertica_load import VerticaCopyTask
import luigi
import luigi.task


class LoadFromSqoop(SqoopImportTask):

    def connection_url(self, _cred):
        pass


class LoadCertificatesDataIntoWarehouse(VerticaCopyTask):

    destination=luigi.Parameter()
    credentials=luigi.Parameter()
    database=luigi.Parameter()
    table=luigi.Parameter()

    @property
    def table(self):
        return "certificates_generatedcertificate"

    @property
    def columns(self):
        return [
            ('user_id', 'INTEGER'),
            ('course_id', 'INTEGER'),
            ('is_certified', 'INTEGER'),
            ('enrollment_mode', 'VARCHAR(255)'),
            ('final_grade', 'VARCHAR(5'),
        ]

    @property
    def insert_source_task(self):
        return LoadFromSqoop(
            destination=self.destination,
            credentials=self.credentials,
            database=self.database,
            tablename=self.tablename
        )

    @property
    def copy_delimiter(self):
        """The delimiter in the data to be copied.  Default is tab (\t). But from sqoop we get comma separated values."""
        return ","