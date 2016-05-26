import luigi

from edx.analytics.tasks.load_internal_reporting_certificates import LoadInternalReportingCertificatesToWarehouse
from edx.analytics.tasks.load_internal_reporting_country import LoadInternalReportingCountryToWarehouse
from edx.analytics.tasks.load_internal_reporting_course import LoadInternalReportingCourseToWarehouse
from edx.analytics.tasks.load_internal_reporting_user_activity import InternalReportingUserActivityWorkflow
from edx.analytics.tasks.load_internal_reporting_user_course import LoadInternalReportingUserCourseToWarehouse
from edx.analytics.tasks.load_internal_reporting_user import LoadInternalReportingUserToWarehouse
from edx.analytics.tasks.course_catalog import CourseCatalogWorkflow
from edx.analytics.tasks.vertica_load import VerticaCopyTaskMixin

import logging
log = logging.getLogger(__name__)


class LoadWarehouse(luigi.WrapperTask):

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()

    interval = luigi.DateIntervalParameter()
    history_schema = luigi.Parameter(default='history')

    user_country_output = luigi.Parameter(
        config_path={'section': 'last-country-of-user', 'name': 'user_country_output'},
        description='Location for intermediate output of location_per_course task.',
    )

    schema = luigi.Parameter()
    credentials = luigi.Parameter()

    overwrite = luigi.BooleanParameter(default=False)

    def requires(self):
        kwargs = {
            'schema': self.schema,
            'credentials': self.credentials,
            'overwrite': self.overwrite,
        }

        yield (
            LoadInternalReportingCertificatesToWarehouse(
                date=self.date,
                **kwargs
            ),
            LoadInternalReportingCountryToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            LoadInternalReportingCourseToWarehouse(
                run_date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            LoadInternalReportingUserCourseToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            InternalReportingUserActivityWorkflow(
                interval=self.interval,
                n_reduce_tasks=self.n_reduce_tasks,
                history_schema=self.history_schema,
                **kwargs
            ),
            LoadInternalReportingUserToWarehouse(
                interval=self.interval,
                user_country_output=self.user_country_output,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            CourseCatalogWorkflow( # Should we use DailyLoadSubjectsToVerticaTask here ?
                run_date=self.date,
                **kwargs
            )
        )

    def output(self):
        return [task.output() for task in self.requires()]
