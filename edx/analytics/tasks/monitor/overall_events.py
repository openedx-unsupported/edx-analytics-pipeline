"""Compute overall event metrics"""

import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin, EventLogSelectionMixinSpark
from edx.analytics.tasks.util.url import get_target_from_url
from luigi.contrib.spark import PySparkTask

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


class SparkTotalEventsDailyTask(EventLogSelectionMixinSpark, PySparkTask):
    """Produce a dataset for total events within a given time period."""

    driver_memory = '2g'
    executor_memory = '3g'

    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(SparkTotalEventsDailyTask, self).__init__(*args, **kwargs)

    def output(self):
        return get_target_from_url(self.output_root)

    def main(self, sc, *args):
        from pyspark.sql import SparkSession
        from pyspark.sql.types import *
        from pyspark.sql.functions import to_date, udf, struct, date_format
        spark = SparkSession.builder.getOrCreate()
        event_schema = StructType().add("POST", StringType(), True).add("GET", StringType(), True)
        module_schema = StructType().add("display_name", StringType(), True) \
            .add("original_usage_key", StringType(), True) \
            .add("original_usage_version", StringType(), True) \
            .add("usage_key", StringType(), True)
        context_schema = StructType().add("command", StringType(), True) \
            .add("course_id", StringType(), True) \
            .add("module", module_schema) \
            .add("org_id", StringType(), True) \
            .add("path", StringType(), True) \
            .add("user_id", StringType(), True)

        event_log_schema = StructType() \
            .add("username", StringType(), True) \
            .add("event_type", StringType(), True) \
            .add("ip", StringType(), True) \
            .add("agent", StringType(), True) \
            .add("host", StringType(), True) \
            .add("referer", StringType(), True) \
            .add("accept_language", StringType(), True) \
            .add("event", event_schema) \
            .add("event_source", StringType(), True) \
            .add("context", context_schema) \
            .add("time", StringType(), True) \
            .add("name", StringType(), True) \
            .add("page", StringType(), True) \
            .add("session", StringType(), True)

        df = spark.read.format('json').load(self.path_targets, schema=event_log_schema)
        df = df.withColumn('event_date', date_format(to_date(df['time']), 'yyyy-MM-dd'))
        df = df.filter(df['event_date'] == self.lower_bound_date_string).groupBy('event_date').count()
        df.repartition(1).write.csv(self.output().path, mode='overwrite', sep='\t')
        # df.repartition(1).rdd.map(lambda row: '\t'.join(map(str, row))).saveAsTextFile(self.output_dir().path)
