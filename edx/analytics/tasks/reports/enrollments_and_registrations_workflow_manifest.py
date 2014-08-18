"""Defines workflow for generating multiple enrollment/registration reports."""

import luigi
import luigi.configuration
import luigi.hdfs

from edx.analytics.tasks.url import url_path_join
from edx.analytics.tasks.reports.enrollments import EnrollmentsByWeek
from edx.analytics.tasks.reports.total_enrollments import AllCourseEnrollmentCountMixin
from edx.analytics.tasks.reports.total_enrollments import WeeklyAllUsersAndEnrollments
from edx.analytics.tasks.reports.incremental_enrollments import WeeklyIncrementalUsersAndEnrollments
from edx.analytics.tasks.reports.incremental_enrollments import DailyRegistrationsEnrollmentsAndCourses


TOTAL_USERS_AND_ENROLLMENTS_NUM_WEEKS = 52
DEFAULT_NUM_DAYS = 28
WEEKLY_ENROLLMENT_REPORT_WEEKS = 10


class EnrollmentsandRegistrationsWorkflow(luigi.Task, AllCourseEnrollmentCountMixin):
    """
    Runs all related tasks to generate enrollment and registration reports


    parameters:
        all parameters required for all tasks and their dependencies,
        many with default values from client.cfg

    """

    def requires(self):
        """
        Runs each task
        """

        output_destination = url_path_join(self.destination, self.name, str(self.date))

        if self.manifest_path is not None:
            manifest = url_path_join(self.manifest_path, "executive-reports", self.name, str(self.date))
        else:
            manifest = None

        common_parameters = {
            "name": self.name,
            "src": self.src,
            "include": self.include,
            "manifest": manifest,
            "credentials": self.credentials,
            "blacklist": self.blacklist,
            "mapreduce_engine": self.mapreduce_engine,
            "lib_jar": self.lib_jar,
            "n_reduce_tasks": self.n_reduce_tasks,
            "destination": output_destination,
            "date": self.date,
        }

        yield (
            WeeklyAllUsersAndEnrollments(
                offsets=self.offsets,
                history=self.history,
                weeks=TOTAL_USERS_AND_ENROLLMENTS_NUM_WEEKS,
                **common_parameters

            ),

            WeeklyIncrementalUsersAndEnrollments(
                offsets=self.offsets,
                history=self.history,
                weeks=WEEKLY_ENROLLMENT_REPORT_WEEKS,
                **common_parameters
            ),

            EnrollmentsByWeek(
                offsets=self.offsets,
                statuses=self.statuses,
                weeks=WEEKLY_ENROLLMENT_REPORT_WEEKS,
                **common_parameters
            ),

            DailyRegistrationsEnrollmentsAndCourses(
                days=DEFAULT_NUM_DAYS,
                **common_parameters
            )
        )
