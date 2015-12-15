"""
Workflow that runs CourseActivityWeekly and internal_reporting_user_activity.
"""
import datetime

import luigi
from edx.analytics.tasks.load_internal_reporting_user_activity import LoadInternalReportingUserActivityToWarehouse
from edx.analytics.tasks.user_activity import CourseActivityWeeklyTask


class UserActivityWorkflow(LoadInternalReportingUserActivityToWarehouse,CourseActivityWeeklyTask):

    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()
    end_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    weeks = luigi.IntParameter(default=24)
    schema = luigi.Parameter()
    warehouse_path = luigi.Parameter()

    @property
    def insert_source_task(self):
        return[
            CourseActivityWeeklyTask(
                end_data=self.end_date,
                weeks=self.weeks,
                n_reduce_tasks=self.n_reduce_tasks,
            ),
            LoadInternalReportingUserActivityToWarehouse(
                interval=self.interval,
                n_reduce_tasks=self.n_reduce_tasks,
                schema=self.schema,
                warehouse_path=self.warehouse_path,
            )
        ]