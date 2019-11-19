"""
Loads multiple insights tables into the warehouse through the pipeline via Hive.
"""
from __future__ import absolute_import

import datetime
import logging
import os

import luigi

from edx.analytics.tasks.common.pathutil import PathSetTask
from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.enterprise.enterprise_enrollments import EnterpriseEnrollmentRecord
from edx.analytics.tasks.enterprise.enterprise_user import EnterpriseUserRecord
from edx.analytics.tasks.insights.enrollments import (
    CourseProgramMetadataRecord, CourseSummaryEnrollmentRecord, EnrollmentByBirthYearRecord,
    EnrollmentByEducationLevelRecord, EnrollmentByGenderRecord, EnrollmentByModeRecord, EnrollmentDailyRecord
)
from edx.analytics.tasks.insights.location_per_course import LastCountryOfUserRecord, LastCountryPerCourseRecord
from edx.analytics.tasks.insights.module_engagement import (
    ModuleEngagementRecord, ModuleEngagementSummaryMetricRangeRecord
)
from edx.analytics.tasks.insights.user_activity import CourseActivityRecord
from edx.analytics.tasks.insights.video import VideoSegmentSummaryRecord, VideoTimelineRecord
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL, url_path_join

log = logging.getLogger(__name__)


class LoadHiveTableToVertica(WarehouseMixin, VerticaCopyTask):
    """
    Generic task to load hive table into Vertica.
    """
    table_name = luigi.Parameter(
        description='Name of hive table to load into vertica.'
    )
    sql_schema = luigi.ListParameter(
        description='Schema of the SQL table.'
    )
    load_from_latest_partition = luigi.Parameter(
        default=True,
        description='Boolean to indicate if data will be loaded from hive partition.'
    )

    def __init__(self, *args, **kwargs):
        super(LoadHiveTableToVertica, self).__init__(*args, **kwargs)
        # Find the most recent data for the source if load from latest partition is enabled.
        if self.load_from_latest_partition:
            path = url_path_join(self.warehouse_path, self.table_name)
            path_targets = PathSetTask([path]).output()
            paths = list(set([os.path.dirname(target.path) for target in path_targets]))
            dates = [path.rsplit('/', 2)[-1] for path in paths]
            latest_date = sorted(dates)[-1]
            self.latest_date = datetime.datetime.strptime(latest_date, "dt=%Y-%m-%d").date()
            log.debug('Loading data for table %s from partition %s', self.table_name, self.latest_date)

    @property
    def insert_source_task(self):
        if self.load_from_latest_partition:
            url = self.hive_partition_path(self.table_name, self.latest_date)
        else:
            url = url_path_join(self.warehouse_path, self.table_name) + '/'
        return ExternalURL(url)

    @property
    def table(self):
        return self.table_name

    @property
    def default_columns(self):
        return None

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return self.sql_schema


class LoadInsightsTableToVertica(WarehouseMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Wrapper task to insert data into Vertica."""

    schema = luigi.Parameter(
        default='insights',
        description='The schema in vertica database to which to write.',
    )

    def requires(self):
        kwargs = {
            'warehouse_path': self.warehouse_path,
            'overwrite': True,
            'schema': self.schema,
            'credentials': self.credentials,
            'read_timeout': self.read_timeout,
            'marker_schema': self.marker_schema,
        }
        yield (
            LoadHiveTableToVertica(
                table_name='course_activity',
                sql_schema=CourseActivityRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_daily',
                sql_schema=EnrollmentDailyRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_birth_year_daily',
                sql_schema=EnrollmentByBirthYearRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_education_level_daily',
                sql_schema=EnrollmentByEducationLevelRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_gender_daily',
                sql_schema=EnrollmentByGenderRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_mode_daily',
                sql_schema=EnrollmentByModeRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_meta_summary_enrollment',
                sql_schema=CourseSummaryEnrollmentRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_program_metadata',
                sql_schema=CourseProgramMetadataRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='course_enrollment_location_current',
                load_from_latest_partition=False,
                sql_schema=LastCountryPerCourseRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='module_engagement',
                sql_schema=ModuleEngagementRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='module_engagement_metric_ranges',
                sql_schema=ModuleEngagementSummaryMetricRangeRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='video_timeline',
                sql_schema=VideoTimelineRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='video',
                sql_schema=VideoSegmentSummaryRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='last_country_of_user_id',
                sql_schema=LastCountryOfUserRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='enterprise_enrollment',
                sql_schema=EnterpriseEnrollmentRecord.get_sql_schema(),
                **kwargs
            ),
            LoadHiveTableToVertica(
                table_name='enterprise_user',
                sql_schema=EnterpriseUserRecord.get_sql_schema(),
                **kwargs
            )
        )
