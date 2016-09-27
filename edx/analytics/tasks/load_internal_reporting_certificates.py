"""
Loads the d_user_course_certificate table into the warehouse through the pipeline via Hive.
"""

import luigi
import logging

from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import HiveTableFromQueryTask, WarehouseMixin, HivePartition
from edx.analytics.tasks.database_imports import ImportGeneratedCertificatesTask

log = logging.getLogger(__name__)


class LoadInternalReportingCertificatesTableHive(HiveTableFromQueryTask):
    """Loads internal_reporting_certificates Hive table from certificates_generatedcertificate Hive table."""

    date = luigi.DateParameter()

    def requires(self):
        return ImportGeneratedCertificatesTask(overwrite=self.overwrite, destination=self.warehouse_path)

    @property
    def table(self):
        return 'internal_reporting_certificates'

    @property
    def columns(self):
        return [
            ('user_id', 'INT'),
            ('course_id', 'STRING'),
            ('is_certified', 'INT'),
            ('certificate_mode', 'STRING'),
            ('final_grade', 'STRING'),
            ('has_passed', 'INT'),
            ('created_date', 'TIMESTAMP'),
            ('modified_date', 'TIMESTAMP'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_query(self):
        return """
            SELECT
              user_id
            , course_id
            , if(status='downloadable', 1, 0) as is_certified
            , mode as certificate_mode
            , grade as final_grade
            , if(status='downloadable' OR status='audit_passing', 1, 0) as has_passed
            , created_date
            , modified_date
            FROM certificates_generatedcertificate
            """


class LoadInternalReportingCertificatesToWarehouse(WarehouseMixin, VerticaCopyTask):
    """
    Loads the internal_reporting_certificates table from Hive into the Vertica data warehouse.
    """

    date = luigi.DateParameter()

    @property
    def table(self):
        return 'd_user_course_certificate'

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
        return [
            ('user_id', 'INTEGER NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('is_certified', 'INTEGER'),
            ('certificate_mode', 'VARCHAR(200)'),
            ('final_grade', 'VARCHAR(5)'),
            ('has_passed', 'INTEGER'),
            ('created_date', 'TIMESTAMP'),
            ('modified_date', 'TIMESTAMP'),
        ]

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return (
            LoadInternalReportingCertificatesTableHive(
                overwrite=self.overwrite,
                warehouse_path=self.warehouse_path,
                date=self.date
            )
        )
