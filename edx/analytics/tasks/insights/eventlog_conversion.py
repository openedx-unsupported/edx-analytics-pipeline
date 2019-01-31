import logging

import luigi.date_interval

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.common.spark import SparkJobTask, SparkMixin
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
    filter_eventlogs = luigi.BoolParameter(
        default=False
    )
    interval = luigi.DateIntervalParameter(
        default=None,
        description='The range of dates to export logs for.',
    )

    def __init__(self, *args, **kwargs):
        super(EventLogConversionSparkTask, self).__init__(*args, **kwargs)
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

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
        from pyspark.sql.functions import to_date, date_format
        df = self._spark.read.format('json').load(self.eventlogs_source)
        if self.filter_eventlogs is not None:
            df = df['time'].isNotNull()
            df = df.withColumn('event_date', date_format(to_date(df['time']), 'yyyy-MM-dd'))
            df = df.filter(
                (df['event_date'] >= self.lower_bound_date_string) &
                (df['event_date'] < self.upper_bound_date_string)
            )
        df.write.parquet(self.output_root)


def get_event_predicate_labels(event_type, event_source):
    """
    Creates labels by applying hardcoded predicates to a single event.
    Don't pass whole event row to any spark UDF as it generates a different output than expected
    """
    # We only want the explicit event, not the implicit form.
    # return 'test'

    ACTIVE_LABEL = "ACTIVE"
    PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
    PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
    POST_FORUM_LABEL = "POSTED_FORUM"
    labels = ACTIVE_LABEL

    # task & enrollment events are filtered out by spark later as it speeds up due to less # of records

    if event_source == 'server':
        if event_type == 'problem_check':
            labels += ',' + PROBLEM_LABEL

        if event_type.startswith('edx.forum.') and event_type.endswith('.created'):
            labels += ',' + POST_FORUM_LABEL

    if event_source in ('browser', 'mobile'):
        if event_type == 'play_video':
            labels += ',' + PLAY_VIDEO_LABEL

    return labels


def get_course_id(event_context, from_url=False):
    """
    Gets course_id from event's data.
    Don't pass whole event row to any spark UDF as it generates a different output than expected
    """
    if event_context == '' or event_context is None:
        # Assume it's old, and not worth logging...
        return ''

    # Get the course_id from the data, and validate.
    raw_course_id = ''
    try:
        raw_course_id = event_context['course_id']
    except KeyError:
        pass
    course_id = opaque_key_util.normalize_course_id(raw_course_id)
    if course_id:
        if opaque_key_util.is_valid_course_id(course_id):
            return course_id

    return ''


class UserActivitySpark(WarehouseMixin, OverwriteOutputMixin, SparkJobTask, SparkMixin):
    """
    UserActivityTask converted to spark ( using event logs in parquet or json format )
    """

    output_root = luigi.Parameter()
    eventlogs_source = luigi.Parameter(
        description='A URL to path that contains log files that contain the json events. (e.g., s3://my_bucket/foo/).',
    )
    eventlogs_format = luigi.Parameter(
        default='parquet'
    )
    interval = luigi.DateIntervalParameter(
        description='The range of dates to export logs for.',
    )
    marker = luigi.Parameter(
        config_path={'section': 'map-reduce', 'name': 'marker'},
        significant=False,
        description='A URL location to a directory where a marker file will be written on task completion.',
    )
    num_coalesce_partitions = luigi.Parameter(
        default=10
    )

    def __init__(self, *args, **kwargs):
        super(UserActivitySpark, self).__init__(*args, **kwargs)
        self.lower_bound_date_string = self.interval.date_a.strftime('%Y-%m-%d')  # pylint: disable=no-member
        self.upper_bound_date_string = self.interval.date_b.strftime('%Y-%m-%d')  # pylint: disable=no-member

    @property
    def spark_remote_package_names(self):
        return ['edx', 'opaque_keys', 'stevedore', 'bson', 'six', 'luigi']

    def get_schema(self):
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

    def spark_job(self, *args):
        from pyspark.sql.functions import to_date, udf, date_format, split, explode, lit
        from pyspark.sql.types import StringType
        get_labels = udf(get_event_predicate_labels, StringType())
        get_courseid = udf(get_course_id, StringType())
        df_reader = self._spark.read.format(self.eventlogs_format)
        if self.eventlogs_format == 'json':
            df_reader = df_reader.schema(self.get_schema())
        df = df_reader.load(self.eventlogs_source)

        df = df.filter(
            (df['time'].isNotNull()) &
            (df['event_source'] != 'task') &
            ~ df['event_type'].startswith('edx.course.enrollment.') &
            (df['context.user_id'].isNotNull())
        )
        df = df.withColumn('event_date', date_format(to_date(df['time']), 'yyyy-MM-dd'))
        df = df.filter(
            (df['event_date'] >= self.lower_bound_date_string) &
            (df['event_date'] < self.upper_bound_date_string)
        )
        # passing complete row to UDF
        df = df.withColumn('all_labels', get_labels(df['event_type'], df['event_source'])) \
            .withColumn('course_id', get_courseid(df['context']))
        df = df.filter(df['course_id'] != '')  # remove rows with empty course_id
        df = df.withColumn('label', explode(split(df['all_labels'], ',')))
        result_df = df.select('context.user_id', 'course_id', 'event_date', 'label') \
            .groupBy('user_id', 'course_id', 'event_date', 'label').count()
        final_df = result_df.withColumn('dt', lit(result_df['event_date']))  # generate extra column for partitioning
        final_df.coalesce(self.num_coalesce_partitions).write.partitionBy('dt').csv(self.output_root, mode='append',
                                                                                    sep='\t')

    def output(self):
        marker_url = url_path_join(self.marker, str(hash(self)))
        return get_target_from_url(marker_url, marker=True)

    def on_success(self):  # pragma: no cover
        """
        Overload the success method to touch the _SUCCESS file.  Any class that uses a separate Marker file from
        the data file will need to override the base on_success() call to create this marker.
        """
        self.output().touch_marker()

    def remove_output_on_overwrite(self):
        super(UserActivitySpark, self).remove_output_on_overwrite()
        if self.overwrite:
            output = get_target_from_url(self.output_root)
            if output.exists():
                output.remove()

    def run(self):
        self.remove_output_on_overwrite()
        super(UserActivitySpark, self).run()
