from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, PathSelectionByDateIntervalTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from luigi.contrib.spark import PySparkTask
from edx.analytics.tasks.util.constants import PredicateLabels


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
            print '\n LOADING via spark context {}\n'.format(zipfile_path)
            self._spark_context.addPyFile(zipfile_path)


    def _load_internal_dependency_on_cluster(self):
        """creates a zip of edx dir and loads it on spark worker nodes"""
        # TODO: delete zipfile after loading on cluster completes
        import os
        import tempfile
        import shutil
        import edx as import_dir_path
        package_name = 'edx_analytics_tasks'
        tmp_dir = tempfile.mkdtemp()
        archive_dir = os.path.join(import_dir_path.__path__[0], '../')  # more reasonable than using path with __file__
        zipfile_path = shutil.make_archive(os.path.join(tmp_dir, package_name), 'zip', archive_dir, 'edx/')
        # add zipfile to spark context
        self._spark_context.addPyFile(zipfile_path)

    def run(self):
        self.remove_output_on_overwrite()
        super(SparkJobTask, self).run()

    def main(self, sc, *args):
        self.init_spark(sc)
        # self._load_internal_dependency_on_cluster()  # load internal dependency for spark worker nodes on cluster
        self._load_external_dependency_on_cluster()  # load external dependency for spark worker nodes on cluster
        self.spark_job()


def get_event_predicate_labels(event):
    """Creates labels by applying hardcoded predicates to a single event."""
    # We only want the explicit event, not the implicit form.
    # return 'test'
    event_type = event['event_type']
    event_source = event['event_source']

    labels = PredicateLabels.ACTIVE_LABEL

    # task & enrollment events are filtered out by spark later as it speeds up due to less # of records

    if event_source == 'server':
        if event_type == 'problem_check':
            labels += ',' + PredicateLabels.PROBLEM_LABEL

        if event_type.startswith('edx.forum.') and event_type.endswith('.created'):
            labels += ',' + PredicateLabels.POST_FORUM_LABEL

    if event_source in ('browser', 'mobile'):
        if event_type == 'play_video':
            labels += ',' + PredicateLabels.PLAY_VIDEO_LABEL

    return labels


def get_key_value_from_event(event, key, default_value=None):
    """
    Get value from event dict by key
    Pyspark does not support dict.get() method, so this approach seems reasonable
    """
    try:
        default_value = event[key]
    except KeyError:
        pass
    return default_value


def get_course_id(event, from_url=False):
    """Gets course_id from event's data."""

    # Get the event data:
    event_context = get_key_value_from_event(event, 'context')
    if event_context is None:
        # Assume it's old, and not worth logging...
        return ''

    # Get the course_id from the data, and validate.
    course_id = opaque_key_util.normalize_course_id(get_key_value_from_event(event_context, 'course_id', ''))
    if course_id:
        if opaque_key_util.is_valid_course_id(course_id):
            return course_id
        else:
            return ''  # we'll filter out empty course since string is expected

    # Try to get the course_id from the URLs in `event_type` (for implicit
    # server events) and `page` (for browser events).
    if from_url:
        source = get_key_value_from_event(event, 'event_source')

        if source == 'server':
            url = get_key_value_from_event(event, 'event_type', '')
        elif source == 'browser':
            url = get_key_value_from_event(event, 'page', '')
        else:
            url = ''

        course_key = opaque_key_util.get_course_key_from_url(url)
        if course_key:
            return unicode(course_key)

    return ''
