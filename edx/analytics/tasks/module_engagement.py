"""Measure student engagement with individual modules in the course"""
from collections import defaultdict
import datetime
import logging
import random

import luigi
import luigi.task
from luigi import date_interval
from luigi.configuration import get_config
from edx.analytics.tasks.calendar_task import CalendarTableTask
from edx.analytics.tasks.database_imports import ImportAuthUserTask, ImportCourseUserGroupUsersTask, \
    ImportAuthUserProfileTask
from edx.analytics.tasks.database_imports import ImportCourseUserGroupTask
from edx.analytics.tasks.elasticsearch_load import ElasticsearchIndexTask
from edx.analytics.tasks.enrollments import CourseEnrollmentTableTask

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, ExternalURL
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import IncrementalMysqlInsertTask, MysqlInsertTask

from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask,
    hive_database_name)
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, DateField, FloatField

log = logging.getLogger(__name__)

try:
    import numpy
except ImportError:
    log.warn('Unable to import numpy')
    numpy = None  # pylint: disable=invalid-name


class ModuleEngagementRecord(Record):
    """Represents a count of interactions performed by a user on a particular entity (usually a module in a course)."""

    course_id = StringField(length=255, nullable=False)
    username = StringField(length=30, nullable=False)
    date = DateField(nullable=False)
    entity_type = StringField(length=10, nullable=False)
    entity_id = StringField(length=255, nullable=False)
    event = StringField(length=30, nullable=False)
    count = IntegerField(nullable=False)


class ModuleEngagementDownstreamMixin(
    WarehouseMixin, MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin
):
    """Common parameters and base classes used to pass parameters through the engagement workflow."""

    # Required parameter
    date = luigi.DateParameter()

    # Override superclass to disable this parameter
    interval = None


class ModuleEngagementDataTask(EventLogSelectionMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    Process the event log and categorize user engagement with various types of content.

    This emits one record for each type of interaction. Note that this is loosely defined. For example, for problems, it
    will emit two records if the problem is correct (one for the "attempt" interaction and one for the "correct attempt"
    interaction).

    This task is intended to be run incrementally and populate a single Hive partition. Although time consuming to
    bootstrap the table, it results in a significantly cleaner workflow. It is much clearer what the success and
    failure conditions are for a task and the management of residual data is dramatically simplified. All of that said,
    Hadoop is not designed to operate like this and it would be significantly more efficient to process a range of dates
    at once. The choice was made to stick with the cleaner workflow since the steady-state is the same for both options
    - in general we will only be processing one day of data at a time.
    """

    # Required parameters
    date = luigi.DateParameter()
    output_root = luigi.Parameter()

    # Override superclass to disable this parameter
    interval = None

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementDataTask, self).__init__(*args, **kwargs)

        self.interval = date_interval.Date.from_date(self.date)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        entity_id, entity_type, user_actions = self.get_user_actions_from_event(event_data, event_source, event_type)

        if not entity_id or not entity_type:
            return

        for action in user_actions:
            record = ModuleEngagementRecord(
                course_id=course_id,
                username=username,
                date=DateField().deserialize_from_string(date_string),
                entity_type=entity_type,
                entity_id=entity_id,
                event=action,
                count=0
            )
            # The count is populated by the reducer, so exclude it from the key.
            record_without_count = record.to_string_tuple()[:-1]
            yield (record_without_count, 1)

    def get_user_actions_from_event(self, event_data, event_source, event_type):
        """
        All of the logic needed to categorize the event.

        Returns: A tuple containing the ID of the entity, the type of entity (video, problem etc) and a list of actions
            that the event represents. A single event may represent several categories of action. A submitted correct
            problem, for example, will return both an attempt action and a completion action.

        """
        entity_id = None
        entity_type = None
        user_actions = []

        if event_type == 'problem_check':
            if event_source == 'server':
                entity_type = 'problem'
                if event_data.get('success', 'incorrect').lower() == 'correct':
                    user_actions.append('completed')

                user_actions.append('attempted')
                entity_id = event_data.get('problem_id')
        elif event_type == 'play_video':
            entity_type = 'video'
            user_actions.append('viewed')
            entity_id = event_data.get('id')
        elif event_type.startswith('edx.forum.'):
            entity_type = 'discussion'
            if event_type.endswith('.created'):
                user_actions.append('contributed')

            entity_id = event_data.get('commentable_id')

        return entity_id, entity_type, user_actions

    def reducer(self, key, values):
        """Count the number of records that share this key."""
        yield ('\t'.join(key), sum(values))

    def output(self):
        return get_target_from_url(self.output_root)

    def run(self):
        self.remove_output_on_overwrite()
        return super(ModuleEngagementDataTask, self).run()


class ModuleEngagementTableTask(BareHiveTableTask):
    """The hive table for this engagement data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'module_engagement'

    @property
    def columns(self):
        return ModuleEngagementRecord.get_hive_schema()


class ModuleEngagementPartitionTask(ModuleEngagementDownstreamMixin, HivePartitionTask):
    """The hive table partition for this engagement data."""

    @property
    def partition_value(self):
        """Use a dynamic partition value based on the date parameter."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return ModuleEngagementTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return ModuleEngagementDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite=self.overwrite,
        )


class ModuleEngagementMysqlTask(ModuleEngagementDownstreamMixin, IncrementalMysqlInsertTask):
    """
    This table is appended to every time this workflow is run, so it is expected to grow to be *very* large. For this
    reason, the records are stored in a clustered index allowing for very fast point queries and date range queries
    for individual users in particular courses.

    This allows us to rapidly generate activity charts over time for small numbers of users.
    """

    @property
    def table(self):
        return "module_engagement"

    @property
    def record_filter(self):
        return "date='{date}'".format(date=self.date.isoformat())  # pylint: disable=no-member

    @property
    def auto_primary_key(self):
        # Instead of using an auto incrementing primary key, we define a custom compound primary key. See keys() defined
        # below. This vastly improves the performance of our most common query pattern.
        return None

    @property
    def columns(self):
        return ModuleEngagementRecord.get_sql_schema()

    @property
    def keys(self):
        # Use a primary key since we will always be pulling these records by course_id, username ordered by date
        # This dramatically speeds up access times at the cost of write speed.

        # From: http://dev.mysql.com/doc/refman/5.6/en/innodb-restrictions.html

        # The InnoDB internal maximum key length is 3500 bytes, but MySQL itself restricts this to 3072 bytes. This
        # limit applies to the length of the combined index key in a multi-column index.

        # The total for this key is:
        #   course_id(255 characters * 3 bytes per utf8 char)
        #   username(30 characters * 3 bytes per utf8 char)
        #   date(3 bytes per DATE)
        #   entity_type(10 characters * 3 bytes per utf8 char)
        #   entity_id(255 characters * 3 bytes per utf8 char)
        #   event(30 characters * 3 bytes per utf8 char)
        #   count(4 bytes per INTEGER)

        # Total = 1747
        return [
            ('PRIMARY KEY', ['course_id', 'username', 'date', 'entity_type', 'entity_id', 'event'])
        ]

    @property
    def insert_source_task(self):
        partition_task = ModuleEngagementPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite,
        )
        return partition_task.data_task


class ModuleEngagementVerticaTask(ModuleEngagementDownstreamMixin, VerticaCopyTask):
    """
    Replicate the hive table in Vertica.

    Note that it is updated incrementally.
    """

    @property
    def insert_source_task(self):
        partition_task = ModuleEngagementPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite,
        )
        return partition_task.data_task

    @property
    def table(self):
        return 'f_module_engagement'

    @property
    def default_columns(self):
        """List of tuples defining name and definition of automatically-filled columns."""
        return None

    @property
    def columns(self):
        return ModuleEngagementRecord.get_sql_schema()


class OptionalVerticaMixin(object):
    """
    If a vertica connection is present, replicate the data there. Otherwise, don't require those insertion tasks.
    """

    vertica_schema = luigi.Parameter(default=None)
    vertica_credentials = luigi.Parameter(default=None)

    def __init__(self, *args, **kwargs):
        super(OptionalVerticaMixin, self).__init__(*args, **kwargs)

        if not self.vertica_credentials:
            self.vertica_credentials = get_config().get('vertica-export', 'credentials', None)

        if not self.vertica_schema:
            self.vertica_schema = get_config().get('vertica-export', 'schema', None)

        self.vertica_enabled = self.vertica_credentials and self.vertica_schema


class ModuleEngagementIntervalTask(
    MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin, WarehouseMixin, OptionalVerticaMixin,
    OverwriteOutputMixin, luigi.WrapperTask
):
    """Compute engagement information over a range of dates and insert the results into Hive, Vertica and MySQL"""

    def requires(self):
        for date in self.interval:
            yield ModuleEngagementPartitionTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
            yield ModuleEngagementMysqlTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
            if self.vertica_enabled:
                yield ModuleEngagementVerticaTask(
                    date=date,
                    n_reduce_tasks=self.n_reduce_tasks,
                    warehouse_path=self.warehouse_path,
                    overwrite=self.overwrite,
                    schema=self.vertica_schema,
                    credentials=self.vertica_credentials,
                )

    def output(self):
        return [task.output() for task in self.requires()]

    def get_raw_data_tasks(self):
        """
        A generator that iterates through all tasks used to generate the data in each partition in the interval.

        This can be used by downstream map reduce jobs to read all of the raw data.
        """
        for date in self.interval:
            partition_task = ModuleEngagementPartitionTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
            yield partition_task.data_task


class ModuleEngagementSummaryRecord(Record):
    """
    Summarizes a user's engagement with a particular course on a particular day with simple counts of activity.
    """

    course_id = StringField()
    username = StringField()
    start_date = DateField()
    end_date = DateField()
    problem_attempts = IntegerField(is_metric=True)
    problems_attempted = IntegerField(is_metric=True)
    problems_completed = IntegerField(is_metric=True)
    problem_attempts_per_completion = FloatField(is_metric=True)
    videos_viewed = IntegerField(is_metric=True)
    discussions_contributed = IntegerField(is_metric=True)
    days_active = IntegerField()

    def get_metrics(self):
        """
        A generator that returns all fields that are metrics.

        Returns: A generator of tuples whose first element is the metric name, and the second is the value of the metric
            for this particular record.
        """
        for field_name, field_obj in self.get_fields().items():
            if getattr(field_obj, 'is_metric', False):
                yield field_name, getattr(self, field_name)


class ModuleEngagementSummaryRecordBuilder(object):
    """Gather the data needed to emit a sparse weekly course engagement record"""

    def __init__(self):
        self.problem_attempts = 0
        self.problems_attempted = set()
        self.problems_completed = set()
        self.videos_viewed = set()
        self.discussions_contributed = 0
        self.days_active = set()


class ModuleEngagementSummaryDataTask(
    ModuleEngagementDownstreamMixin, OptionalVerticaMixin, OverwriteOutputMixin, MapReduceJobTask
):
    """
    Store a summary of student engagement with their courses on particular dates aggregated using a sliding window.

    Only emits a record if the user did something in the course on that particular day, this dramatically reduces the
    volume of data in this table and keeps it manageable.

    Currently this analyzes a sliding one week window of data to generate the summary.
    """

    # NOTE: We could have done this in Hive, but this MR job is much more simple than the necessary Hive query.
    # particularly given the nuance of how some of the metrics are aggregated (like attempts per completion).

    output_root = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementSummaryDataTask, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)

    def requires_hadoop(self):
        # The hadoop task only wants to read the raw Hive partitions, so only use them as input to the job.
        return list(self.requires().get_raw_data_tasks())

    def mapper(self, line):
        record = ModuleEngagementRecord.from_tsv(line)
        yield ((record.course_id, record.username), line.rstrip('\r\n'))

    def reducer(self, key, lines):
        """Calculate counts for events corresponding to user and course in a given time period."""
        course_id, username = key

        output_record = ModuleEngagementSummaryRecordBuilder()
        for line in lines:
            record = ModuleEngagementRecord.from_tsv(line)

            output_record.days_active.add(record.date)

            count = int(record.count)
            if record.entity_type == 'problem':
                if record.event == 'attempted':
                    output_record.problem_attempts += count
                    output_record.problems_attempted.add(record.entity_id)
                elif record.event == 'completed':
                    output_record.problems_completed.add(record.entity_id)
            elif record.entity_type == 'video':
                if record.event == 'viewed':
                    output_record.videos_viewed.add(record.entity_id)
            elif record.entity_type == 'discussion':
                output_record.discussions_contributed += count
            else:
                log.warn('Unrecognized entity type: %s', record.entity_type)

        attempts_per_completion = self.compute_attempts_per_completion(
            output_record.problem_attempts,
            len(output_record.problems_completed)
        )

        yield ModuleEngagementSummaryRecord(
            course_id,
            username,
            self.interval.date_a,
            self.interval.date_b,
            output_record.problem_attempts,
            len(output_record.problems_attempted),
            len(output_record.problems_completed),
            attempts_per_completion,
            len(output_record.videos_viewed),
            output_record.discussions_contributed,
            len(output_record.days_active)
        ).to_string_tuple()

    @staticmethod
    def compute_attempts_per_completion(num_problem_attempts, num_problems_completed):
        """
        The ratio of attempts per correct problem submission is an indicator of how much a student is struggling.

        If a student has not completed any problems a value of float('inf') is returned.
        """
        if num_problems_completed > 0:
            attempts_per_completion = float(num_problem_attempts) / num_problems_completed
        else:
            attempts_per_completion = float('inf')
        return attempts_per_completion

    def output(self):
        return get_target_from_url(self.output_root)

    def requires(self):
        return ModuleEngagementIntervalTask(
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
        )

    def run(self):
        self.remove_output_on_overwrite()
        return super(ModuleEngagementSummaryDataTask, self).run()


class ModuleEngagementSummaryTableTask(BareHiveTableTask):
    """The hive table for this summary of engagement data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'module_engagement_summary'

    @property
    def columns(self):
        return ModuleEngagementSummaryRecord.get_hive_schema()


class ModuleEngagementSummaryPartitionTask(
    ModuleEngagementDownstreamMixin, OptionalVerticaMixin, HivePartitionTask
):
    """The hive partition for this summary of engagement data."""

    @property
    def partition_value(self):
        """Partition based on the end date of the sliding week-long interval."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return ModuleEngagementSummaryTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return ModuleEngagementSummaryDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
        )


class ModuleEngagementSummaryMetricRangeRecord(Record):
    """
    Metrics are analyzed to determine interesting ranges.

    This could really be any arbitrary range, maybe just the 3rd percentile, or perhaps it could define two groups,
    below and above the median.
    """

    course_id = StringField(length=255, nullable=False)
    start_date = DateField()
    end_date = DateField()
    metric = StringField(length=50, nullable=False)
    range_type = StringField(length=50, nullable=False)
    low_value = FloatField()
    high_value = FloatField()


METRIC_RANGE_HIGH = 'high'
METRIC_RANGE_LOW = 'low'


class ModuleEngagementSummaryMetricRangesDataTask(
    ModuleEngagementDownstreamMixin, OptionalVerticaMixin, OverwriteOutputMixin, MapReduceJobTask
):
    """
    Summarize the metrics of interest and persist the interesting ranges.

    This task currently computes "low" and "high" ranges. The "low" range is defined as the [min(), 15th percentile)
    interval. The "high" range is defined as the [85th percentile, max()). There is an implied "middle" range that is
    not persisted since it is fully defined by the low and the high. Note that all intervals are left-closed.
    """

    EXPAND_RANGE_BY = 0.1

    output_root = luigi.Parameter()
    low_percentile = luigi.FloatParameter(default=15.0)
    high_percentile = luigi.FloatParameter(default=85.0)

    def requires(self):
        # NOTE: The hadoop job needs the raw data to use as input, not the hive partition metadata, which is the output
        # of the partition task.
        partition_task = ModuleEngagementSummaryPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
        )
        return partition_task.data_task

    def mapper(self, line):
        record = ModuleEngagementSummaryRecord.from_tsv(line)
        yield record.course_id, line.rstrip('\n')

    def reducer(self, course_id, lines):
        """
        Analyze all summary records for particular course.

        This will include all students performed any activity of interest in the past week.
        """
        metric_values = defaultdict(list)

        first_record = None
        for line in lines:
            record = ModuleEngagementSummaryRecord.from_tsv(line)
            if first_record is None:
                # There is some information we need to copy out of the summary records, so just grab one of them. There
                # will be at least one, or else the reduce function would have never been called.
                first_record = record
            for metric, value in record.get_metrics():
                if value != 0:
                    # NOTE: a lot of people don't participate, so don't include them in the analysis. Otherwise it would
                    # look like doing *anything* in the course meant you were doing really well.
                    metric_values[metric].append(value)

        for metric in sorted(metric_values):
            values = metric_values[metric]
            range_values = numpy.percentile(  # pylint: disable=no-member
                values, [0.0, self.low_percentile, self.high_percentile, 100.0]
            )
            min_value = range_values[0] - self.EXPAND_RANGE_BY
            max_value = range_values[3] + self.EXPAND_RANGE_BY
            ranges = [
                (METRIC_RANGE_LOW, min_value, range_values[1]),
                (METRIC_RANGE_HIGH, range_values[2], max_value),
            ]
            for range_type, low_value, high_value in ranges:
                yield ModuleEngagementSummaryMetricRangeRecord(
                    course_id=course_id,
                    start_date=first_record.start_date,
                    end_date=first_record.end_date,
                    metric=metric,
                    range_type=range_type,
                    low_value=low_value,
                    high_value=high_value
                ).to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)

    def extra_modules(self):
        return [numpy]

    def run(self):
        self.remove_output_on_overwrite()
        return super(ModuleEngagementSummaryMetricRangesDataTask, self).run()


class ModuleEngagementSummaryMetricRangesTableTask(BareHiveTableTask):
    """Hive table for metric ranges"""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'module_engagement_metric_ranges'

    @property
    def columns(self):
        return ModuleEngagementSummaryMetricRangeRecord.get_hive_schema()


class ModuleEngagementSummaryMetricRangesPartitionTask(
    ModuleEngagementDownstreamMixin, OptionalVerticaMixin, HivePartitionTask
):
    """Hive partition for metric ranges"""

    @property
    def partition_value(self):
        """Use the end date as the partition identifier"""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return ModuleEngagementSummaryMetricRangesTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return ModuleEngagementSummaryMetricRangesDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
        )


class ModuleEngagementSummaryMetricRangesMysqlTask(ModuleEngagementDownstreamMixin, MysqlInsertTask):
    """Result store storage for the metric ranges."""

    overwrite = luigi.BooleanParameter(default=True)

    @property
    def table(self):
        return "module_engagement_metric_ranges"

    @property
    def auto_primary_key(self):
        return None

    @property
    def columns(self):
        return ModuleEngagementSummaryMetricRangeRecord.get_sql_schema()

    @property
    def keys(self):
        # Store the records in order by course_id to allow for very fast access for point queries by course.
        return [
            ('PRIMARY KEY', ['course_id', 'metric', 'range_type'])
        ]

    @property
    def insert_source_task(self):
        partition_task = ModuleEngagementSummaryMetricRangesPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite,
        )
        return partition_task.data_task


class ModuleEngagementUserSegmentRecord(Record):
    """
    Maps a user's activity in a course to various segments.
    """

    course_id = StringField()
    username = StringField()
    start_date = DateField()
    end_date = DateField()
    segment = StringField()
    reason = StringField()


SEGMENT_HIGHLY_ENGAGED = 'highly_engaged'
SEGMENT_STRUGGLING = 'struggling'


class ModuleEngagementUserSegmentDataTask(
    ModuleEngagementDownstreamMixin, OptionalVerticaMixin, OverwriteOutputMixin, MapReduceJobTask
):
    """
    Segment the user population in each course using their activity data.

    Note that a particular user may belong to many different segments. Also note that this will not be an exhaustive
    list of segments. Some segments are based on the absence of activity data, which cannot be computed here since this
    job only reads records for users that performed *some* activity in the course.

    Looks at the last week of activity summary records for each user and assigns the following segments:

        struggling: The user has a value for problem_attempts_per_completion that is in the top 15% of all non-zero
            values.
        highly_engaged: The user is in the top 15% for any of the metrics except problem_attempts
            and problem_attempts_per_completion.
    """

    output_root = luigi.Parameter()

    def requires_local(self):
        # NOTE: Most of this table is pickled on the master node and distributed to the slaves via the distributed
        # cache.
        return ModuleEngagementSummaryMetricRangesPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
        ).data_task

    def requires_hadoop(self):
        yield ModuleEngagementSummaryPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
        ).data_task

    def init_local(self):
        super(ModuleEngagementUserSegmentDataTask, self).init_local()
        self.high_metric_ranges = defaultdict(dict)

        # Grab all of the "high" ranges since those are the only ones we are going to use. This will load
        # O(num_courses * num_metrics) records into memory, and then into the pickled job instance. This should be a
        # relatively small number (thousands).
        with self.input_local().open('r') as metric_ranges_target:
            for line in metric_ranges_target:
                range_record = ModuleEngagementSummaryMetricRangeRecord.from_tsv(line)
                if range_record.range_type == METRIC_RANGE_HIGH:
                    self.high_metric_ranges[range_record.course_id][range_record.metric] = range_record

    def mapper(self, line):
        record = ModuleEngagementSummaryRecord.from_tsv(line)
        yield (record.course_id, record.username), line.rstrip('\n')

    def reducer(self, key, lines):
        course_id, username = key

        records = [ModuleEngagementSummaryRecord.from_tsv(line) for line in lines]

        # Maps segment names to a set of "reasons" that tell why the user was placed in that segment
        segments = defaultdict(set)
        most_recent_summary = records[-1]
        for metric, value in most_recent_summary.get_metrics():
            high_metric_range = self.high_metric_ranges.get(course_id, {}).get(metric)
            if high_metric_range is None or metric == 'problem_attempts':
                continue

            # Typically a left-closed interval, however, we consider infinite values to be included in the interval
            # if the upper bound is infinite.
            value_less_than_high = (
                (value < high_metric_range.high_value)
                or (value in (float('inf'), float('-inf')) and high_metric_range.high_value == value)
            )
            if (high_metric_range.low_value <= value) and value_less_than_high:
                if metric == 'problem_attempts_per_completion':
                    # A high value for this metric actually indicates a struggling student
                    segments[SEGMENT_STRUGGLING].add(metric)
                else:
                    segments[SEGMENT_HIGHLY_ENGAGED].add(metric)

        for segment in sorted(segments):
            yield ModuleEngagementUserSegmentRecord(
                course_id=course_id,
                username=username,
                start_date=records[0].start_date,
                end_date=records[0].end_date,
                segment=segment,
                reason=','.join(segments[segment])
            ).to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)

    def run(self):
        self.remove_output_on_overwrite()
        return super(ModuleEngagementUserSegmentDataTask, self).run()


class ModuleEngagementUserSegmentTableTask(BareHiveTableTask):
    """Hive table for user segment assignments."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'module_engagement_user_segments'

    @property
    def columns(self):
        return ModuleEngagementUserSegmentRecord.get_hive_schema()


class ModuleEngagementUserSegmentPartitionTask(
    ModuleEngagementDownstreamMixin, OptionalVerticaMixin, HivePartitionTask
):
    """Hive partition for user segment assignments."""

    @property
    def partition_value(self):
        return self.date.isoformat()

    @property
    def hive_table_task(self):
        return ModuleEngagementUserSegmentTableTask(
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite,
        )

    @property
    def data_task(self):
        return ModuleEngagementUserSegmentDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
        )


class ModuleEngagementRosterRecord(Record):
    course_id = StringField()
    username = StringField()
    start_date = DateField()
    end_date = DateField()
    email = StringField()
    name = StringField(analyzed=True)
    enrollment_mode = StringField()
    enrollment_date = DateField()
    cohort = StringField()
    problem_attempts = IntegerField()
    problems_attempted = IntegerField()
    problems_completed = IntegerField()
    problem_attempts_per_completion = FloatField()
    videos_viewed = IntegerField()
    discussions_contributed = IntegerField()
    segments = StringField(analyzed=True)
    attempt_ratio_order = IntegerField()


class ModuleEngagementRosterTableTask(BareHiveTableTask):

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'module_engagement_roster'

    @property
    def columns(self):
        return ModuleEngagementRosterRecord.get_hive_schema()


class ModuleEngagementRosterPartitionTask(ModuleEngagementDownstreamMixin, OptionalVerticaMixin, HivePartitionTask):

    date = luigi.DateParameter()
    interval = None
    partition_value = None

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementRosterPartitionTask, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)
        self.partition_value = self.date.isoformat()

    def query(self):
        # Join with calendar data only if calculating weekly engagement.
        last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
        iso_weekday = last_complete_date.isoweekday()

        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec}) {if_not_exists}
        SELECT
            ce.course_id,
            au.username,
            '{start}',
            '{end}',
            au.email,
            regexp_replace(regexp_replace(aup.name, '\\\\t|\\\\n|\\\\r', ' '), '\\\\\\\\', ''),
            ce.mode,
            lce.last_enrollment_date,
            cohort.name,
            COALESCE(eng.problem_attempts, 0),
            COALESCE(eng.problems_attempted, 0),
            COALESCE(eng.problems_completed, 0),
            eng.problem_attempts_per_completion,
            COALESCE(eng.videos_viewed, 0),
            COALESCE(eng.discussions_contributed, 0),
            CONCAT_WS(
                ",",
                IF(ce.at_end = 0, "unenrolled", NULL),
                IF(COALESCE(old_eng.days_active, 0) = 0 AND COALESCE(eng.days_active, 0) = 0, "inactive", NULL),
                IF(COALESCE(old_eng.days_active, 0) > 0 AND COALESCE(eng.days_active, 0) = 0, "disengaging", NULL),
                seg.segments
            ),
            IF(
                eng.problem_attempts_per_completion > 1 OR eng.problem_attempts_per_completion IS NULL,
                COALESCE(eng.problem_attempts, 0),
                -COALESCE(eng.problem_attempts, 0)
            )
        FROM course_enrollment ce
        INNER JOIN calendar cal ON (ce.date = cal.date)
        INNER JOIN auth_user au
            ON (ce.user_id = au.id)
        INNER JOIN auth_userprofile aup
            ON (au.id = aup.user_id)
        LEFT OUTER JOIN (
            SELECT
                cugu.user_id,
                cug.course_id,
                cug.name
            FROM course_groups_courseusergroup_users cugu
            INNER JOIN course_groups_courseusergroup cug
                ON (cugu.courseusergroup_id = cug.id)
        ) cohort
            ON (au.id = cohort.user_id AND ce.course_id = cohort.course_id)
        LEFT OUTER JOIN module_engagement_summary eng
            ON (ce.course_id = eng.course_id AND au.username = eng.username AND eng.end_date = '{end}')
        LEFT OUTER JOIN module_engagement_summary old_eng
            ON (ce.course_id = old_eng.course_id AND au.username = old_eng.username AND old_eng.end_date = DATE_SUB('{end}', 7))
        LEFT OUTER JOIN (
            SELECT
                course_id,
                user_id,
                MAX(date) AS last_enrollment_date
            FROM course_enrollment
            WHERE
                at_end = 1 AND date < '{end}'
            GROUP BY course_id, user_id
        ) lce
            ON (ce.course_id = lce.course_id AND ce.user_id = lce.user_id)
        LEFT OUTER JOIN (
            SELECT
                course_id,
                username,
                CONCAT_WS(",", COLLECT_SET(segment)) AS segments
            FROM module_engagement_user_segments
            WHERE end_date = '{end}'
            GROUP BY course_id, username
        ) seg
            ON (ce.course_id = seg.course_id AND au.username = seg.username)
        WHERE
            ce.date >= '{start}'
            AND ce.date < '{end}'
            AND cal.iso_weekday = {iso_weekday}
        """.format(
            start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
            end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
            iso_weekday=iso_weekday,
            partition=self.partition,
            if_not_exists='' if self.overwrite else 'IF NOT EXISTS',
            database_name=hive_database_name(),
            table=self.hive_table_task.table,
        )
        return query

    @property
    def hive_table_task(self):
        return ModuleEngagementRosterTableTask(
            warehouse_path=self.warehouse_path,
        )

    def requires(self):
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
            'import_date': self.date
        }
        yield (
            self.hive_table_task,
            ModuleEngagementUserSegmentPartitionTask(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
            ),
            ModuleEngagementSummaryPartitionTask(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
            ),
            ModuleEngagementSummaryPartitionTask(
                date=(self.date - datetime.timedelta(weeks=1)),
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
            ),
            CourseEnrollmentTableTask(
                interval_end=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
            ),
            CalendarTableTask(),
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportCourseUserGroupTask(**kwargs_for_db_import),
            ImportCourseUserGroupUsersTask(**kwargs_for_db_import),
            ImportAuthUserProfileTask(**kwargs_for_db_import),
        )


class ModuleEngagementRosterIndexTask(
    ModuleEngagementDownstreamMixin,
    OptionalVerticaMixin,
    ElasticsearchIndexTask
):
    index = luigi.Parameter(
        config_path={'section': 'module-engagement', 'name': 'index'}
    )
    number_of_shards = luigi.Parameter(
        config_path={'section': 'module-engagement', 'name': 'number_of_shards'}
    )
    obfuscate = luigi.BooleanParameter(default=False)
    scale_factor = luigi.IntParameter(default=1)

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementRosterIndexTask, self).__init__(*args, **kwargs)

        self.other_reduce_tasks = self.n_reduce_tasks
        if self.indexing_tasks is not None:
            self.n_reduce_tasks = self.indexing_tasks

    @property
    def partition_task(self):
        return ModuleEngagementRosterPartitionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.other_reduce_tasks,
            overwrite=self.overwrite,
            date=self.date,
        )

    def requires_local(self):
        return self.partition_task

    def input_hadoop(self):
        return get_target_from_url(self.partition_task.partition_location)

    @property
    def properties(self):
        return ModuleEngagementRosterRecord.get_elasticsearch_properties()

    @property
    def doc_type(self):
        return 'roster_entry'

    def document_generator(self, lines):
        for line in lines:
            record = ModuleEngagementRosterRecord.from_tsv(line)

            if self.obfuscate:
                import names
                email = '{0}@example.com'.format(record.username)
                n = random.randrange(5)
                if n == 0:
                    gender = random.choice(('male', 'female'))
                    name = '{0} {1} {2}'.format(
                        names.get_first_name(gender, cached=True),
                        names.get_first_name(gender, cached=True),
                        names.get_last_name(cached=True)
                    )
                else:
                    name = names.get_full_name(cached=True)
            else:
                email = record.email
                name = record.name

            document = {
                '_id': '|'.join([record.course_id, record.username]),
                '_source': {
                    'name': name,
                    'email': email,
                }
            }

            for maybe_null_field in ModuleEngagementRosterRecord.get_fields():
                if maybe_null_field in ('name', 'email'):
                    continue
                maybe_null_value = getattr(record, maybe_null_field)
                if maybe_null_value is not None and maybe_null_field != float('inf'):
                    document['_source'][maybe_null_field] = maybe_null_value

            original_id = document['_id']
            for i in range(self.scale_factor):
                if i > 0:
                    document['_id'] = original_id + '|' + str(i)
                yield document

    def extra_modules(self):
        packages = super(ModuleEngagementRosterIndexTask, self).extra_modules()

        if self.obfuscate:
            import names
            packages.append(names)

        return packages
