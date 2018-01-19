from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, PathSelectionByDateIntervalTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from luigi.contrib.spark import PySparkTask


class EventLogSelectionMixinSpark(EventLogSelectionDownstreamMixin):
    """
    Extract events corresponding to a specified time interval.
    """
    path_targets = None

    def __init__(self, *args, **kwargs):
        """
        Call path selection task to get list of log files matching the pattern
        """
        super(EventLogSelectionDownstreamMixin, self).__init__(*args, **kwargs)
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member
        path_targets = PathSelectionByDateIntervalTask(
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            date_pattern=self.date_pattern,
        ).output()
        self.path_targets = [task.path for task in path_targets]

    def get_log_schema(self):
        """
        Get spark based schema for processing event logs
        :return: Spark schema
        """
        from pyspark.sql.types import StructType, StringType
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

        return event_log_schema

    def get_event_log_dataframe(self, spark, *args, **kwargs):
        from pyspark.sql.functions import to_date, udf, struct, date_format
        dataframe = spark.read.format('json').load(self.path_targets, schema=self.get_log_schema())
        dataframe = dataframe.filter(dataframe['time'].isNotNull()) \
            .withColumn('event_date', date_format(to_date(dataframe['time']), 'yyyy-MM-dd'))
        dataframe = dataframe.filter(dataframe['event_date'] == self.lower_bound_date_string)
        return dataframe


class SparkJobTask(OverwriteOutputMixin, PySparkTask):
    """
    Wrapper for spark task
    """

    _spark = None
    _spark_context = None
    _sql_context = None
    _hive_context = None

    driver_memory = '2g'
    executor_memory = '3g'

    def init_spark(self, sc):
        """
        Initialize spark, sql and hive context
        :param sc: Spark context
        """
        from pyspark.sql import SparkSession, SQLContext, HiveContext
        self._sql_context = SQLContext(sc)
        self._spark_context = sc
        self._spark = SparkSession.builder.getOrCreate()
        self._hive_context = HiveContext(sc)

    def spark_job(self):
        """
        Spark code for the job
        """
        raise NotImplementedError

    def _load_external_dependency_on_cluster(self):
        """creates a zip of packages and loads it on spark worker nodes"""
        import importlib
        import os
        import tempfile
        from zipfile import ZipFile, ZIP_DEFLATED
        packages = self.py_packages
        if not packages:
            return
        tmp_dir = tempfile.mkdtemp()

        def zipdir(path, ziphandle, package_name):
            if os.path.isfile(mod_path):
                ziphandle.write(path, os.path.basename(path))
            else:
                for root, dirs, files in os.walk(path):
                    for file in files:
                        ziphandle.write(os.path.join(root, file),
                                   os.path.relpath(os.path.join(root, file), os.path.join(path, '..')))

        for package in packages:
            mod = importlib.import_module(package)
            try:
                mod_path = mod.__path__[0]
            except AttributeError:
                if mod.__file__[-4:] == '.pyc':
                    mod_path = mod.__file__[:-1]
                else:
                    mod_path = mod.__file__
            zipfile_path = os.path.join(tmp_dir, package + '.zip')
            zipfile = ZipFile(zipfile_path, 'w', ZIP_DEFLATED)
            zipdir(mod_path, zipfile, package)
            zipfile.close()
            self._spark_context.addPyFile(zipfile_path)


    def _load_internal_dependency_on_cluster(self):
        """creates a zip of edx and luigi package and loads it on spark worker nodes"""
        # TODO: delete zipfile after loading on cluster completes
        import os
        import tempfile
        import shutil
        import edx as import_dir_path
        import luigi
        tmp_dir = tempfile.mkdtemp()
        # zip edx package
        archive_dir = os.path.join(import_dir_path.__path__[0], '../')
        zipfile_edx = shutil.make_archive(os.path.join(tmp_dir, 'edx_analytics_tasks'), 'zip', archive_dir, 'edx/')
        # zip luigi package
        archive_dir = os.path.join(luigi.__path__[0], '../')
        zipfile_luigi = shutil.make_archive(os.path.join(tmp_dir, 'luigi'), 'zip', archive_dir, 'luigi/')
        # add zipfile to spark context
        self._spark_context.addPyFile(zipfile_edx)
        self._spark_context.addPyFile(zipfile_luigi)

    def run(self):
        self.remove_output_on_overwrite()
        super(SparkJobTask, self).run()

    def main(self, sc, *args):
        self.init_spark(sc)
        self._load_internal_dependency_on_cluster()  # load internal dependency for spark worker nodes on cluster
        # self._load_external_dependency_on_cluster()  # load external dependency for spark worker nodes on cluster
        self.spark_job()
