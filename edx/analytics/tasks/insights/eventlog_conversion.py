import logging

import luigi.date_interval

from edx.analytics.tasks.common.spark import SparkJobTask
from edx.analytics.tasks.util.hive import WarehouseMixin
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class EventLogConversionSparkTask(WarehouseMixin, OverwriteOutputMixin, SparkJobTask):
    """
    Spark task to convert json event logs to columnar format.
    New format will include all columns, column filtering will be done later on.
    """

    output_root = luigi.Parameter()
    marker = luigi.Parameter(
        config_path={'section': 'map-reduce', 'name': 'marker'},
        significant=False,
        description='A URL location to a directory where a marker file will be written on task completion.',
    )
    eventlogs_source = luigi.Parameter(
        description='A URL to path that contains log files that contain the json events. (e.g., s3://my_bucket/foo/).',
    )

    def output(self):
        marker_url = url_path_join(self.marker, str(hash(self)))
        return get_target_from_url(marker_url, marker=True)

    def on_success(self):  # pragma: no cover
        self.output().touch_marker()

    def remove_output_on_overwrite(self):
        super(EventLogConversionSparkTask, self).remove_output_on_overwrite()
        if self.overwrite:
            output = get_target_from_url(self.output_root)
            if output.exists():
                output.remove()

    def run(self):
        self.remove_output_on_overwrite()
        super(EventLogConversionSparkTask, self).run()

    def spark_job(self, *args):
        df = self._spark.read.format('json').load(self.eventlogs_source)
        df.write.parquet(self.output_root)
