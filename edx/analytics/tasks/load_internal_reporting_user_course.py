"""
Loads the user_course table into the warehouse through the pipeline via Hive.
"""
import luigi
import logging

from edx.analytics.tasks.vertica_load import (
    VerticaCopyTask, VerticaProjection, PROJECTION_TYPE_NORMAL, PROJECTION_TYPE_AGGREGATE
)
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition
from edx.analytics.tasks.url import url_path_join, ExternalURL

log = logging.getLogger(__name__)


class LoadInternalReportingUserCourseToWarehouse(WarehouseMixin, VerticaCopyTask):
    """Loads the course_enrollment Hive table into the f_user_course table in Vertica data warehouse."""

    date = luigi.DateParameter()
    n_reduce_tasks = luigi.Parameter()

    @property
    def partition(self):
        """The table is partitioned by date."""
        return HivePartition('dt', self.date.isoformat())  # pylint: disable=no-member

    @property
    def insert_source_task(self):
        hive_table = "course_enrollment"
        partition_location = url_path_join(self.warehouse_path, hive_table, self.partition.path_spec) + '/'
        return ExternalURL(url=partition_location)

    @property
    def table(self):
        return 'f_user_course'

    @property
    def columns(self):
        """The schema has enrollment_is_active as well, but 'course_enrollment' hive table does not have it."""
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(256) NOT NULL'),
            ('user_id', 'INTEGER NOT NULL'),
            ('enrollment_is_active', 'INTEGER'),
            ('enrollment_change', 'INTEGER'),
            ('enrollment_mode', 'VARCHAR(100)')
        ]

    @property
    def auto_primary_key(self):
        """Use 'record_number' as primary key to match the schema"""
        return ('record_number', 'AUTO_INCREMENT')

    @property
    def default_columns(self):
        return None

    @property
    def projections(self):
        return [
            VerticaProjection(
                "{schema}.{table}_projection_1",
                PROJECTION_TYPE_NORMAL,
                """
(
 record_number ENCODING COMMONDELTA_COMP,
 user_id ENCODING RLE,
 course_id ENCODING RLE,
 date ENCODING COMMONDELTA_COMP,
 enrollment_mode ENCODING AUTO,
 enrollment_is_active ENCODING RLE,
 enrollment_change ENCODING BLOCKDICT_COMP
)
AS
 SELECT record_number,
        user_id,
        course_id,
        date,
        enrollment_mode,
        enrollment_is_active,
        enrollment_change
 FROM {schema}.{table}
 ORDER BY course_id,
          enrollment_is_active,
          user_id,
          record_number
UNSEGMENTED ALL NODES;"""
            ),
            VerticaProjection(
                "{schema}.{table}_projection_2",
                PROJECTION_TYPE_NORMAL,
                """
(
 user_id ENCODING RLE,
 course_id ENCODING RLE,
 date ENCODING RLE,
 enrollment_mode ENCODING RLE,
 enrollment_is_active ENCODING RLE
)
AS
 SELECT user_id,
        course_id,
        date,
        enrollment_mode,
        enrollment_is_active
 FROM {schema}.{table}
 ORDER BY enrollment_is_active,
          date,
          course_id,
          enrollment_mode,
          user_id
UNSEGMENTED ALL NODES;"""
            ),
            VerticaProjection(
                "{schema}.{table}_projection_3",
                PROJECTION_TYPE_NORMAL,
                """
(
 course_id ENCODING RLE,
 date ENCODING RLE,
 enrollment_mode ENCODING RLE,
 enrollment_is_active ENCODING RLE
)
AS
 SELECT course_id,
        date,
        enrollment_mode,
        enrollment_is_active
 FROM {schema}.{table}
 ORDER BY date,
          course_id,
          enrollment_is_active,
          enrollment_mode
UNSEGMENTED ALL NODES;"""
            ),
            VerticaProjection(
                "{schema}.{table}_first_enrollment",
                PROJECTION_TYPE_AGGREGATE,
                """
(
 enrollment_is_active ENCODING RLE,
 course_id ENCODING RLE,
 user_id ENCODING AUTO,
 first_enrolled ENCODING AUTO
)
AS
 SELECT enrollment_is_active,
        course_id,
        user_id,
        MIN(date)
 FROM {schema}.{table}
     GROUP BY 1, 2, 3;"""
            ),
            VerticaProjection(
                "{schema}.{table}_first_enrollment_by_type",
                PROJECTION_TYPE_AGGREGATE,
                """
(
 enrollment_is_active ENCODING RLE,
 enrollment_mode ENCODING RLE,
 course_id ENCODING RLE,
 user_id ENCODING AUTO,
 first_enrolled ENCODING AUTO
)
AS
 SELECT enrollment_is_active,
        enrollment_mode,
        course_id,
        user_id,
        MIN(date)
 FROM {schema}.{table}
 GROUP BY 1, 2, 3, 4;"""
            ),
        ]
