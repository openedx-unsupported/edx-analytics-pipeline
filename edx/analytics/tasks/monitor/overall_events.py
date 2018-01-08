"""Compute overall event metrics"""

import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.common.spark import EventLogSelectionMixinSpark, SparkJobTask
from edx.analytics.tasks.util.url import get_target_from_url

log = logging.getLogger(__name__)


class TotalEventsDailyTask(EventLogSelectionMixin, MapReduceJobTask):
    """Produce a dataset for total events within a given time period."""

    output_root = luigi.Parameter()

    def mapper(self, line):
        event, date_string = self.get_event_and_date_string(line) or (None, None)
        if event is None:
            return

        yield (date_string), 1

    def reducer(self, key, values):
        """Emit counts for each day."""

        count = sum(values)
        yield key, count

    # Take advantage of a combiner to optimize network hops
    combiner = reducer

    def output(self):
        return get_target_from_url(self.output_root)


class SparkTotalEventsDailyTask(EventLogSelectionMixinSpark, SparkJobTask):
    """Produce a dataset for total events within a given time period."""

    driver_memory = '2g'
    executor_memory = '3g'

    output_root = luigi.Parameter()

    def output(self):
        return get_target_from_url(self.output_root)

    def spark_job(self):
        from pyspark.sql.functions import to_date, udf, struct, date_format
        df = self._spark.read.format('json').load(self.path_targets, schema=self.get_log_schema())
        df = df.filter(df['time'].isNotNull()) \
            .withColumn('event_date', date_format(to_date(df['time']), 'yyyy-MM-dd'))
        df = df.filter(df['event_date'] == self.lower_bound_date_string).groupBy('event_date').count()
        df.repartition(1).write.csv(self.output().path, mode='overwrite', sep='\t')
        # df.repartition(1).rdd.map(lambda row: '\t'.join(map(str, row))).saveAsTextFile(self.output_dir().path)
