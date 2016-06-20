"""
Workflow to load the warehouse, this serves as a replacement for pentaho loading.
"""
import logging
import luigi

from edx.analytics.tasks.load_internal_reporting_certificates import LoadInternalReportingCertificatesToWarehouse
from edx.analytics.tasks.load_internal_reporting_country import LoadInternalReportingCountryToWarehouse
from edx.analytics.tasks.load_internal_reporting_course import LoadInternalReportingCourseToWarehouse
from edx.analytics.tasks.load_internal_reporting_user_activity import LoadInternalReportingUserActivityToWarehouse
from edx.analytics.tasks.load_internal_reporting_user_course import LoadInternalReportingUserCourseToWarehouse
from edx.analytics.tasks.load_internal_reporting_user import LoadInternalReportingUserToWarehouse
from edx.analytics.tasks.course_catalog import DailyLoadSubjectsToVerticaTask

from edx.analytics.tasks.util.hive import WarehouseMixin

log = logging.getLogger(__name__)


class LoadWarehouse(WarehouseMixin, luigi.WrapperTask):
    """Runs the workflow needed to load warehouse."""

    date = luigi.DateParameter()

    n_reduce_tasks = luigi.Parameter()

    # We are not using VerticaCopyTaskMixin as OverwriteOutputMixin changes the complete() method behavior.
    schema = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'schema'},
        description='The schema to which to write.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )

    overwrite = luigi.BooleanParameter(default=False)

    def requires(self):
        kwargs = {
            'schema': self.schema,
            'credentials': self.credentials,
            'overwrite': self.overwrite,
            'warehouse_path': self.warehouse_path,
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
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            LoadInternalReportingUserCourseToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            LoadInternalReportingUserActivityToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            LoadInternalReportingUserToWarehouse(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                **kwargs
            ),
            DailyLoadSubjectsToVerticaTask(
                date=self.date,
                **kwargs
            )
        )

    def output(self):
        return [task.output() for task in self.requires()]
