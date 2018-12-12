"""
Loads the user_course table into the warehouse through the pipeline via Hive.
"""
import datetime
import logging

import luigi

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask, VerticaCopyTaskMixin
from edx.analytics.tasks.insights.video import VideoSegmentDetailRecord, VideoSegmentSummaryRecord, VideoTimelineRecord
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.url import ExternalURL

log = logging.getLogger(__name__)


class LoadVideoSegmentDetailToVertica(WarehouseMixin, VerticaCopyTask):
    """
    Load the video usage (segment detail) table into Vertica.
    """
    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('video_usage', self.date))

    @property
    def table(self):
        return 'video_segment_detail'

    @property
    def default_columns(self):
        return None

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return VideoSegmentDetailRecord.get_sql_schema()


class LoadVideoSegmentSummaryToVertica(WarehouseMixin, VerticaCopyTask):
    """
    Load the video (segment summary) table into Vertica.
    """
    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path('video', self.date))

    @property
    def table(self):
        return 'video_segment_summary'

    @property
    def default_columns(self):
        return None

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return VideoSegmentSummaryRecord.get_sql_schema()


class LoadVideoTimelineToVertica(WarehouseMixin, VerticaCopyTask):
    """
    Load the video timeline table into Vertica.
    """
    date = luigi.DateParameter()

    @property
    def insert_source_task(self):
        return ExternalURL(url=self.hive_partition_path(self.table, self.date))

    @property
    def table(self):
        return 'video_timeline'

    @property
    def default_columns(self):
        return None

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return VideoTimelineRecord.get_sql_schema()


class LoadAllVideoToVertica(WarehouseMixin, VerticaCopyTaskMixin, luigi.WrapperTask):
    """Insert all video data into Vertica."""

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Default is today, UTC.',
    )

    def requires(self):
        kwargs = {
            'date': self.date,
            'warehouse_path': self.warehouse_path,
            'overwrite': self.overwrite,
            'schema': self.schema,
            'credentials': self.credentials,
            'read_timeout': self.read_timeout,
            'marker_schema': self.marker_schema,
        }
        yield (
            LoadVideoSegmentDetailToVertica(**kwargs),
            LoadVideoSegmentSummaryToVertica(**kwargs),
            LoadVideoTimelineToVertica(**kwargs),
        )
