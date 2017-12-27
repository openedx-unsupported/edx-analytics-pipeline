"""Compute overall event metrics"""

import logging

import luigi

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionMixin
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

# TAKE THIS OVER TO TEST USE OF SPARK:

# modified from the Luigi example code, which is rather old.

from luigi.contrib.spark import SparkSubmitTask, PySparkTask


class InlinePySparkWordCount(PySparkTask):
    """
    This task runs a :py:class:`luigi.contrib.spark.PySparkTask` task
    over the target data in :py:meth:`wordcount.input` (a file in S3) and
    writes the result into its :py:meth:`wordcount.output` target (a file in S3).
    This class uses :py:meth:`luigi.contrib.spark.PySparkTask.main`.
    Example luigi configuration::
        [spark]
        spark-submit: /usr/local/spark/bin/spark-submit
        master: spark://spark.example.org:7077
        # py-packages: numpy, pandas
    """
    driver_memory = '2g'
    executor_memory = '3g'

    input_file = luigi.Parameter()
    output_file = luigi.Parameter()

    def input(self):
        return get_target_from_url(self.input_file)

    def output(self):
        return get_target_from_url(self.output_file)

    def main(self, sc, *args):
        sc.textFile(self.input().path) \
          .flatMap(lambda line: line.split()) \
          .map(lambda word: (word, 1)) \
          .reduceByKey(lambda a, b: a + b) \
          .saveAsTextFile(self.output().path)
