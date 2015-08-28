"""
Loads the user_course table into the warehouse through the pipeline via Hive.

"""
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.enrollments import CourseEnrollmentTableTask
import luigi
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition

class LoadInternalReportingUserCourseToWarehouse(WarehouseMixin, VerticaCopyTask):

    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        return (
            CourseEnrollmentTableTask(
                mapreduce_engine = self.mapreduce_engine,
                n_reduce_tasks = self.n_reduce_tasks,
                source = self.source,
                interval_end = self.interval_end,
                pattern = self.pattern,
                overwrite = True
            )
        )

    @property
    def table(self):
        return 'f_user_course'

    @property
    def columns(self):
        """The schema has enrollment_is_active as well, but 'course_enrollment' hive table does not have it."""
        return [
            ('date', 'DATE'),
            ('course_id', 'VARCHAR(200)'),
            ('user_id', 'INTEGER'),
            ('enrollment_change', 'INTEGER'),
            ('enrollment_mode', 'VARCHAR(100)')
        ]
