"""
Loads the user_course table into the warehouse through the pipeline via Hive.
"""
import luigi
import logging

from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.url import url_path_join, ExternalURL

log = logging.getLogger(__name__)


class LoadInternalReportingUserCourseToWarehouse(WarehouseMixin, VerticaCopyTask):
    """Loads the course_enrollment Hive table into the f_user_course table in Vertica data warehouse."""

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        hive_table = "course_enrollment"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

    @property
    def table(self):
        return 'f_user_course'

    @property
    def columns(self):
        """The schema has enrollment_is_active as well, but 'course_enrollment' hive table does not have it."""
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(256) NOT NULL'),
            ('user_id', 'INTEGER NOT NULL'),
            ('enrollment_is_active', 'INTEGER'),
            ('enrollment_change', 'INTEGER'),
            ('enrollment_mode', 'VARCHAR(100)')
        ]

    @property
    def auto_primary_key(self):
        """Use 'record_number' as primary key to match the schema"""
        return ('record_number', 'AUTO_INCREMENT')

    @property
    def default_columns(self):
        return None
