"""
Workflow that runs CourseActivityWeekly and internal_reporting_user_activity.
"""
import datetime

import luigi
from edx.analytics.tasks.load_internal_reporting_user_activity import LoadInternalReportingUserActivityToWarehouse, \
    AggregateInternalReportingUserActivityTableHive
from edx.analytics.tasks.user_activity import CourseActivityWeeklyTask


class UserActivityWorkflow(luigi.WrapperTask):

    interval = luigi.DateIntervalParameter()
    n_reduce_tasks = luigi.Parameter()
    end_date = luigi.DateParameter(default=datetime.datetime.utcnow().date())
    weeks = luigi.IntParameter(default=24)

    def requires(self):
        return[
            CourseActivityWeeklyTask(
                end_date=self.end_date,
                weeks=self.weeks,
                n_reduce_tasks=self.n_reduce_tasks,
            ),
            AggregateInternalReportingUserActivityTableHive(
                interval=self.interval,
                n_reduce_tasks=self.n_reduce_tasks,
            )
        ]