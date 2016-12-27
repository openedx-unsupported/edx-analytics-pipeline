"""
Loads the user_course table into the warehouse through the pipeline via Hive.
"""
import luigi
import logging

from edx.analytics.tasks.enrollments import EnrollmentSummaryRecord
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.url import ExternalURL

log = logging.getLogger(__name__)


class LoadUserCourseSummary(WarehouseMixin, VerticaCopyTask):
    """
    Load the course enrollment summary table into vertica.
    """
    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('course_enrollment_summary', self.date))

    @property
    def table(self):
        return 'd_user_course'

    @property
    def default_columns(self):
        return None

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return EnrollmentSummaryRecord.get_sql_schema()
