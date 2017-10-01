"""
Loads the internal reporting course block structure into the warehouse through the pipeline.
"""

import logging

from edx.analytics.tasks.common.vertica_load import VerticaCopyTask
from edx.analytics.tasks.insights.course_blocks import CourseBlocksDownstreamMixin, CourseBlocksPartitionTask, CourseBlockRecord


# pylint: disable=anomalous-unicode-escape-in-string

log = logging.getLogger(__name__)


class LoadInternalReportingCourseBlocksToWarehouse(CourseBlocksDownstreamMixin, VerticaCopyTask):

    """Loads the course_blocks tsv into the Vertica data warehouse."""

    @property
    def insert_source_task(self):
        return CourseBlocksPartitionTask(
            date=self.date,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
            input_root=self.input_root,
        ).data_task

    @property
    def default_columns(self):
        return []

    @property
    def table(self):
        return 'course_blocks'

    @property
    def columns(self):
        return CourseBlockRecord.get_sql_schema()
