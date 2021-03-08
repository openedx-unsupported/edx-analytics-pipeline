"""
Measure student engagement with individual modules in the course.

See ModuleEngagementWorkflowTask for more extensive documentation.
"""

import datetime
import logging
import random
from collections import defaultdict

import luigi.task
from luigi import date_interval

from edx.analytics.tasks.common.elasticsearch_load import ElasticsearchIndexTask
from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.common.mysql_load import IncrementalMysqlInsertTask, MysqlInsertTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserProfileTask, ImportAuthUserTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask
)
from edx.analytics.tasks.insights.enrollments import ExternalCourseEnrollmentPartitionTask
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.decorators import workflow_entry_point
from edx.analytics.tasks.util.hive import BareHiveTableTask, HivePartitionTask, WarehouseMixin, hive_database_name
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, FloatField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

try:
    import numpy
except ImportError:
    numpy = None  # pylint: disable=invalid-name


log = logging.getLogger(__name__)


class ModuleEngagementRecord(Record):
    """Represents a count of interactions performed by a user on a particular entity (usually a module in a course)."""

    course_id = StringField(length=255, nullable=False, description='Course the learner interacted with.')
    username = StringField(length=30, nullable=False, description='Learner\'s username.')
    date = DateField(nullable=False, description='The learner interacted with the entity on this date.')
    entity_type = StringField(length=10, nullable=False, description='Category of entity that the learner interacted'
                                                                     ' with. Example: "video".')
    entity_id = StringField(length=255, nullable=False, truncate=True,
                            description='A unique identifier for the entity within the'
                                        ' course that the learner interacted with.')
    event = StringField(length=30, nullable=False, description='The interaction the learner had with the entity.'
                                                               ' Example: "viewed".')
    count = IntegerField(nullable=False, description='Number of interactions the learner had with this entity on this'
                                                     ' date.')


class OverwriteFromDateMixin(object):
    """Supports overwriting a subset of the data to compensate for late events."""

    overwrite_from_date = luigi.DateParameter(
        description='This parameter is passed down to the module engagement model which will overwrite data from a date'
                    ' in the past up to the end of the interval. Events are not always collected at the time that they'
                    ' are emitted, sometimes much later. By re-processing past days we can gather late events and'
                    ' include them in the computations.',
        default=None,
        significant=False
    )


class ModuleEngagementDownstreamMixin(WarehouseMixin, MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin,
                                      OverwriteFromDateMixin):
    """Common parameters and base classes used to pass parameters through the engagement workflow."""

    # Required parameter
    date = luigi.DateParameter(
        description='Upper bound date for the end of the interval to analyze. Data produced before 00:00 on this'
                    ' date will be analyzed. This workflow is intended to run nightly and this parameter is intended'
                    ' to be set to "today\'s" date, so that all of yesterday\'s data is included and none of today\'s.'
    )

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

    # Optional parameters
    store_anonymous_username = luigi.Parameter(
        description='Set this parameter to a string which will not overlap with a valid username, e.g. "ANONYMOUS USER".'
                    ' Must be <= 30 characters.',
        default='',
        config_path={'section': 'module-engagement', 'name': 'store_anonymous_username'},
    )

    # Override superclass to disable this parameter
    interval = None

    # Write the output directly to the final destination and rely on the _SUCCESS file to indicate whether or not it
    # is complete. Note that this is a custom extension to luigi.
    enable_direct_output = True

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementDataTask, self).__init__(*args, **kwargs)

        self.interval = date_interval.Date.from_date(self.date)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()

        # Skip anonymous records, unless configured to store them
        if not username:
            if not self.store_anonymous_username:
                return
            username = self.store_anonymous_username

        if len(username) > 30:
            # User retirement process generates usernames containing 54
            # characters, at the time of writing.  It's unusual that an event
            # would be emitted with a retired username, but the least we can do
            # is skip these events.
            return

        event_type = event.get('event_type')
        if event_type is None:
            return

        course_id = eventlog.get_course_id(event, from_url=True)
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
            entity_id = event_data.get('id', '').strip()  # We have seen id values with leading newlines.
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

    def complete(self):
        if self.overwrite and not self.attempted_removal:
            return False
        else:
            return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        self.remove_output_on_overwrite()
        output_target = self.output()
        if not self.complete() and output_target.exists():
            output_target.remove()
        if self.overwrite:
            self.remove_manifest_target_if_exists()
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
    reason, the records are indexed to allow for very fast point queries and date range queries for individual users in
    particular courses.

    This allows us to rapidly generate activity charts over time for small numbers of users.

    Note that it would have been better to use a clustered index and stored the records in sorted order, however, the
    Django ORM does not support composite primary key indexes, so we have to use a secondary index.
    """

    allow_empty_insert = luigi.BoolParameter(
        default=False,
        config_path={'section': 'module-engagement', 'name': 'allow_empty_insert'},
    )
    overwrite_hive = luigi.BoolParameter(
        default=False,
        significant=False
    )

    def update_id(self):
        """
        Use the task name and date to uniquely identify this update.

        This means that other significant parameters will be ignored when testing to see if this task is complete.
        This is a deliberate choice since we really only care about the date. We should only execute this task if the
        data for that date has never been loaded or if overwrite is True. Any other changes to parameters should not
        cause a re-insert.

        Without this, changes to any parameter would result in duplicate records being inserted into the database since
        the hash code would change for the task, so it would be marked incomplete. Since records are only deleted if
        overwrite is specified, then it was relatively easy to insert duplicate records into the database.
        """
        return '{task_name}(date={key})'.format(task_name=self.task_family, key=self.date.isoformat())

    @property
    def table(self):
        return "module_engagement"

    @property
    def record_filter(self):
        return "date='{date}'".format(date=self.date.isoformat())  # pylint: disable=no-member

    @property
    def columns(self):
        return ModuleEngagementRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id', 'username', 'date'),
            ('username', 'entity_type'),
            ('date',),
        ]

    @property
    def insert_source_task(self):
        partition_task = ModuleEngagementPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=self.overwrite_hive,
        )
        return partition_task.data_task


class ModuleEngagementIntervalTask(MapReduceJobTaskMixin, EventLogSelectionDownstreamMixin, WarehouseMixin,
                                   OverwriteOutputMixin, OverwriteFromDateMixin, luigi.WrapperTask):
    """Compute engagement information over a range of dates and insert the results into Hive and MySQL"""

    overwrite_mysql = luigi.BoolParameter(
        default=False,
        significant=False
    )

    def requires(self):
        for date in reversed([d for d in self.interval]):  # pylint: disable=not-an-iterable
            should_overwrite = date >= self.overwrite_from_date
            yield ModuleEngagementPartitionTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=should_overwrite,
            )
            yield ModuleEngagementMysqlTask(
                date=date,
                n_reduce_tasks=self.n_reduce_tasks,
                warehouse_path=self.warehouse_path,
                overwrite=should_overwrite or self.overwrite_mysql,
                overwrite_hive=should_overwrite
            )

    def output(self):
        return [task.output() for task in self.requires()]

    def get_raw_data_tasks(self):
        """
        A generator that iterates through all tasks used to generate the data in each partition in the interval.

        This can be used by downstream map reduce jobs to read all of the raw data.
        """
        for task in self.requires():
            if isinstance(task, ModuleEngagementPartitionTask):
                yield task.data_task


class ModuleEngagementSummaryRecord(Record):
    """
    Summarizes a user's engagement with a particular course in the past week with simple counts of activity.
    """

    course_id = StringField(description='Course the learner interacted with.')
    username = StringField(description='Learner\'s username.')
    start_date = DateField(description='Analysis includes all data from 00:00 on this day up to the end date.')
    end_date = DateField(description='Analysis includes all data up to but not including this date.')
    problem_attempts = IntegerField(is_metric=True, description='Number of times the learner attempted any problem in'
                                                                ' the course.')
    problems_attempted = IntegerField(is_metric=True, description='Number of unique problems the learner has ever'
                                                                  ' attempted in the course.')
    problems_completed = IntegerField(is_metric=True, description='Number of unique problems the learner has ever'
                                                                  ' completed correctly in the course.')
    problem_attempts_per_completed = FloatField(is_metric=True, description='Ratio of the number of attempts the'
                                                                            ' learner has made on any problem to the'
                                                                            ' number of unique problems they have'
                                                                            ' completed correctly in the course.')
    videos_viewed = IntegerField(is_metric=True, description='Number of unique videos the learner has watched any part'
                                                             ' of in the course.')
    discussion_contributions = IntegerField(is_metric=True, description='Total number of posts, responses and comments'
                                                                        ' the learner has made in the course.')
    days_active = IntegerField(description='Number of days the learner performed any activity in.')

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
        self.discussion_contributions = 0
        self.days_active = set()

    def add_record(self, record):
        """
        Updates metrics based on the provided record.

        Arguments:
            record (ModuleEngagementRecord): The record to aggregate.
        """
        self.days_active.add(record.date)

        count = int(record.count)
        if record.entity_type == 'problem':
            if record.event == 'attempted':
                self.problem_attempts += count
                self.problems_attempted.add(record.entity_id)
            elif record.event == 'completed':
                self.problems_completed.add(record.entity_id)
        elif record.entity_type == 'video':
            if record.event == 'viewed':
                self.videos_viewed.add(record.entity_id)
        elif record.entity_type == 'discussion':
            self.discussion_contributions += count
        else:
            log.warn('Unrecognized entity type: %s', record.entity_type)

    def get_summary_record(self, course_id, username, interval):
        """
        Given all of the records that have been added, generate a summarizing record.

        Arguments:
            course_id (string):
            username (string):
            interval (luigi.date_interval.DateInterval):

        Returns:
            ModuleEngagementSummaryRecord: Representing the aggregated summary of all of the learner's activity.
        """
        attempts_per_completion = self.compute_attempts_per_completion(
            self.problem_attempts,
            len(self.problems_completed)
        )

        return ModuleEngagementSummaryRecord(
            course_id,
            username,
            interval.date_a,
            interval.date_b,
            self.problem_attempts,
            len(self.problems_attempted),
            len(self.problems_completed),
            attempts_per_completion,
            len(self.videos_viewed),
            self.discussion_contributions,
            len(self.days_active)
        )

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


class WeekIntervalMixin(object):
    """
    For tasks that accept a date parameter that represents the end date of a week.

    The date is used to set an `interval` attribute that represents the complete week.
    """

    def __init__(self, *args, **kwargs):
        super(WeekIntervalMixin, self).__init__(*args, **kwargs)

        start_date = self.date - datetime.timedelta(weeks=1)
        self.interval = date_interval.Custom(start_date, self.date)


class ModuleEngagementSummaryDataTask(WeekIntervalMixin, ModuleEngagementDownstreamMixin, OverwriteOutputMixin,
                                      MapReduceJobTask):
    """
    Store a summary of student engagement with their courses on particular dates aggregated using a sliding window.

    Only emits a record if the user did something in the course on that particular day. This dramatically reduces the
    volume of data in this table and keeps it manageable.

    Currently this analyzes a sliding one week window of data to generate the summary.
    """

    # NOTE: We could have done this in Hive, but this MR job is much more simple than the necessary Hive query.
    # particularly given the nuance of how some of the metrics are aggregated (like attempts per completion).

    output_root = luigi.Parameter()

    def requires_local(self):
        return ModuleEngagementIntervalTask(
            interval=self.interval,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite_from_date=self.overwrite_from_date,
        )

    def requires_hadoop(self):
        # The hadoop task only wants to read the raw Hive partitions, so only use them as input to the job.
        return list(self.requires_local().get_raw_data_tasks())

    def mapper(self, line):
        record = ModuleEngagementRecord.from_tsv(line)
        yield ((record.course_id, record.username), line.rstrip('\r\n'))

    def reducer(self, key, lines):
        """Calculate counts for events corresponding to user and course in a given time period."""
        course_id, username = key

        output_record_builder = ModuleEngagementSummaryRecordBuilder()
        for line in lines:
            record = ModuleEngagementRecord.from_tsv(line)

            output_record_builder.add_record(record)

        yield output_record_builder.get_summary_record(course_id, username, self.interval).to_string_tuple()

    def output(self):
        return get_target_from_url(self.output_root)

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


class ModuleEngagementSummaryPartitionTask(ModuleEngagementDownstreamMixin, HivePartitionTask):
    """The hive partition for this summary of engagement data."""

    @property
    def partition_value(self):
        """Partition based on the end date of the sliding week-long interval."""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return ModuleEngagementSummaryTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return ModuleEngagementSummaryDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite_from_date=self.overwrite_from_date,
        )


class ModuleEngagementSummaryMetricRangeRecord(Record):
    """
    Metrics are analyzed to determine interesting ranges.

    This could really be any arbitrary range, maybe just the 3rd percentile, or perhaps it could define two groups,
    below and above the median.
    """

    course_id = StringField(length=255, nullable=False, description='Course the learner interacted with.')
    start_date = DateField(description='Analysis includes all data from 00:00 up to the end date.')
    end_date = DateField(description='Analysis includes all data up to, but not including this date.')
    metric = StringField(length=50, nullable=False, description='Metric that this range applies to.')
    range_type = StringField(length=50, nullable=False, description='Type of range. For example: "low"')
    low_value = FloatField(description='Low value for the range. Exact matches are included in the range.')
    high_value = FloatField(description='High value for the range. Exact matches are excluded from the range.')


METRIC_RANGE_HIGH = 'high'
METRIC_RANGE_NORMAL = 'normal'
METRIC_RANGE_LOW = 'low'


class ModuleEngagementSummaryMetricRangesDataTask(ModuleEngagementDownstreamMixin, OverwriteOutputMixin,
                                                  MapReduceJobTask):
    """
    Summarize the metrics of interest and persist the interesting ranges.

    This task currently computes "low", "normal" and "high" ranges. The "low" range is defined as the [0, 15th
    percentile) interval. The "normal" range is defined as [15th percentile, 85th percentile). The "high" range is
    defined as the [85th percentile, float('inf')). Note that all intervals are left-closed. Some edge cases result in
    different ranges being emitted. For example, if only one discrete value is found for a particular metric (everyone
    watched exactly one video or no videos ), it will emit a low range of [0, num_video_views) and a normal range of
    [num_video_views, float('inf')). This will result in any user who watched no videos being categorized as "low" and
    any user who watched one video will be characterized as "normal".
    """

    output_root = luigi.Parameter()
    low_percentile = luigi.FloatParameter(default=15.0)
    high_percentile = luigi.FloatParameter(default=85.0)

    def requires(self):
        # NOTE: The hadoop job needs the raw data to use as input, not the hive partition metadata, which is the output
        # of the partition task.
        partition_task = ModuleEngagementSummaryPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite_from_date=self.overwrite_from_date,
        )
        return partition_task.data_task

    def mapper(self, line):
        record = ModuleEngagementSummaryRecord.from_tsv(line)
        yield record.course_id, line.rstrip('\n')

    def reducer(self, course_id, lines):
        """
        Analyze all summary records for a particular course.

        This will include all students who performed any activity of interest in the past week.
        """
        metric_values = defaultdict(list)

        unprocessed_metrics = set()
        first_record = None
        for line in lines:
            record = ModuleEngagementSummaryRecord.from_tsv(line)
            if first_record is None:
                # There is some information we need to copy out of the summary records, so just grab one of them. There
                # will be at least one, or else the reduce function would have never been called.
                first_record = record

            # don't include inactive learners in metric range computations
            if record.days_active == 0:
                continue

            for metric, value in record.get_metrics():
                unprocessed_metrics.add(metric)
                if metric == 'problem_attempts_per_completed' and record.problem_attempts == 0:
                    # The learner needs to have at least attempted one problem in order for their float('inf') to be
                    # included in the metric ranges. If the ratio is 0/0 we ignore the record.
                    continue
                metric_values[metric].append(value)

        for metric in sorted(metric_values):
            unprocessed_metrics.remove(metric)
            values = metric_values[metric]
            normal_lower_bound, normal_upper_bound = numpy.percentile(  # pylint: disable=no-member
                values, [self.low_percentile, self.high_percentile]
            )
            if numpy.isnan(normal_lower_bound):
                normal_lower_bound = float('inf')
            if numpy.isnan(normal_upper_bound):
                normal_upper_bound = float('inf')
            ranges = []
            if normal_lower_bound > 0:
                ranges.append((METRIC_RANGE_LOW, 0, normal_lower_bound))

            if normal_lower_bound == normal_upper_bound:
                ranges.append((METRIC_RANGE_NORMAL, normal_lower_bound, float('inf')))
            else:
                ranges.append((METRIC_RANGE_NORMAL, normal_lower_bound, normal_upper_bound))
                ranges.append((METRIC_RANGE_HIGH, normal_upper_bound, float('inf')))

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

        for metric in unprocessed_metrics:
            yield ModuleEngagementSummaryMetricRangeRecord(
                course_id=course_id,
                start_date=first_record.start_date,
                end_date=first_record.end_date,
                metric=metric,
                range_type='normal',
                low_value=0,
                high_value=float('inf')
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


class ModuleEngagementSummaryMetricRangesPartitionTask(ModuleEngagementDownstreamMixin, HivePartitionTask):
    """Hive partition for metric ranges"""

    @property
    def partition_value(self):
        """Use the end date as the partition identifier"""
        return self.date.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return ModuleEngagementSummaryMetricRangesTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return ModuleEngagementSummaryMetricRangesDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite_from_date=self.overwrite_from_date,
        )


class ModuleEngagementSummaryMetricRangesMysqlTask(ModuleEngagementDownstreamMixin, MysqlInsertTask):
    """Result store storage for the metric ranges."""

    overwrite = luigi.BoolParameter(
        default=True,
        description='Overwrite the table when writing to it by default. Allow users to override this behavior if they '
                    'want.',
        significant=False
    )
    allow_empty_insert = luigi.BoolParameter(
        default=False,
        config_path={'section': 'module-engagement', 'name': 'allow_empty_insert'},
    )

    @property
    def table(self):
        return "module_engagement_metric_ranges"

    @property
    def columns(self):
        return ModuleEngagementSummaryMetricRangeRecord.get_sql_schema()

    @property
    def indexes(self):
        return [
            ('course_id', 'metric'),
        ]

    @property
    def insert_source_task(self):
        partition_task = ModuleEngagementSummaryMetricRangesPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite=self.overwrite,
            overwrite_from_date=self.overwrite_from_date,
        )
        return partition_task.data_task


class ModuleEngagementUserSegmentRecord(Record):
    """
    Maps a user's activity in a course to various segments.
    """

    course_id = StringField(description='Course the learner is enrolled in.')
    username = StringField(description='Learner\'s username.')
    start_date = DateField(description='Analysis includes all data from 00:00 on this day up to the end date.')
    end_date = DateField(description='Analysis includes all data up to but not including this date.')
    segment = StringField(description='A short term that includes only lower case characters and underscores that'
                                      ' indicates a group that the user belongs to. For example: highly_engaged.')
    reason = StringField(description='A human readable description of the reason for the student being placed in this'
                                     ' segment.')


SEGMENT_HIGHLY_ENGAGED = 'highly_engaged'
SEGMENT_STRUGGLING = 'struggling'


class ModuleEngagementUserSegmentDataTask(ModuleEngagementDownstreamMixin, OverwriteOutputMixin, MapReduceJobTask):
    """
    Segment the user population in each course using their activity data.

    Note that a particular user may belong to many different segments. Also note that this will not be an exhaustive
    list of segments. Some segments are based on the absence of activity data, which cannot be computed here since this
    job only reads records for users that performed *some* activity in the course.

    Looks at the last week of activity summary records for each user and assigns the following segments:

        struggling: The user has a value for problem_attempts_per_completed that is in the top 15% of all non-zero
            values.
        highly_engaged: The user is in the top 15% for any of the metrics except problem_attempts
            and problem_attempts_per_completed.
    """

    output_root = luigi.Parameter()

    def requires_local(self):
        # Parts of this table are read into memory in init_local and distributed to slaves via the distributed cache.
        mysql_task = ModuleEngagementSummaryMetricRangesMysqlTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite_from_date=self.overwrite_from_date,
        )
        return {
            'range_data': mysql_task.insert_source_task,
            'range_mysql': mysql_task,
        }

    def requires_hadoop(self):
        return ModuleEngagementSummaryPartitionTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            overwrite_from_date=self.overwrite_from_date,
        ).data_task

    def init_local(self):
        super(ModuleEngagementUserSegmentDataTask, self).init_local()

        # This structure is used in the reducer, so it is stored on the job object which is serialized on the master and
        # deserialized on the slave nodes. This is default luigi behavior.
        self.high_metric_ranges = defaultdict(dict)

        # Grab all of the "high" ranges since those are the only ones we are going to use. This will load
        # O(num_courses * num_metrics) records into memory, and then into the pickled job instance. This should be a
        # relatively small number (thousands).
        with self.input_local()['range_data'].open('r') as metric_ranges_target:
            for line in metric_ranges_target:
                range_record = ModuleEngagementSummaryMetricRangeRecord.from_tsv(line)
                if range_record.range_type == METRIC_RANGE_HIGH:
                    self.high_metric_ranges[range_record.course_id][range_record.metric] = range_record

    def mapper(self, line):
        record = ModuleEngagementSummaryRecord.from_tsv(line)
        yield (record.course_id, record.username), line.rstrip('\n')

    def reducer(self, key, lines):
        """Given a particular user in a particular course, look at their summary and assign appropriate segments."""
        course_id, username = key

        records = [ModuleEngagementSummaryRecord.from_tsv(line) for line in lines]

        if len(records) > 1:
            raise RuntimeError('There should be exactly one summary record per user per course.')

        summary = records[0]

        # Maps segment names to a set of "reasons" that tell why the user was placed in that segment
        segments = defaultdict(set)
        for metric, value in summary.get_metrics():
            high_metric_range = self.high_metric_ranges.get(course_id, {}).get(metric)
            if high_metric_range is None or metric == 'problem_attempts':
                continue

            # Typically a left-closed interval, however, we consider infinite values to be included in the interval
            # if the upper bound is infinite.
            value_less_than_high = (
                (value < high_metric_range.high_value) or
                (value in (float('inf'), float('-inf')) and high_metric_range.high_value == value)
            )
            if (high_metric_range.low_value <= value) and value_less_than_high:
                if metric == 'problem_attempts_per_completed':
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


class ModuleEngagementUserSegmentPartitionTask(ModuleEngagementDownstreamMixin, HivePartitionTask):
    """Hive partition for user segment assignments."""

    @property
    def partition_value(self):
        """Use the date as the partition value."""
        return self.date.isoformat()

    @property
    def hive_table_task(self):
        return ModuleEngagementUserSegmentTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return ModuleEngagementUserSegmentDataTask(
            date=self.date,
            n_reduce_tasks=self.n_reduce_tasks,
            output_root=self.partition_location,
            overwrite_from_date=self.overwrite_from_date,
        )


class ModuleEngagementRosterRecord(Record):
    """A summary of statistics related to a single learner in a single course related to their engagement."""
    course_id = StringField(description='Course the learner is enrolled in.')
    username = StringField(description='Learner\'s username.')
    start_date = DateField(description='Analysis includes all data from 00:00 on this day up to the end date.')
    end_date = DateField(description='Analysis includes all data up to but not including this date.')
    email = StringField(description='Learner\'s email address.')
    name = StringField(analyzed=True, description='Learner\'s full name including first, middle and last names. '
                                                  'This field can be searched by instructors.')
    enrollment_mode = StringField(description='Learner is enrolled in the course with this mode. Example: verified.')
    enrollment_date = DateField(description='First date the learner enrolled in the course.')
    cohort = StringField(description='Cohort the learner belongs to, can be null.')
    problem_attempts = IntegerField(description='Number of times the learner attempted any problem in the course.')
    problems_attempted = IntegerField(description='Number of unique problems the learner has ever attempted in the'
                                                  ' course.')
    problems_completed = IntegerField(description='Number of unique problems the learner has ever completed correctly'
                                                  ' in the course.')
    problem_attempts_per_completed = FloatField(description='Ratio of the number of attempts the learner has made on'
                                                            ' any problem to the number of unique problems they have'
                                                            ' completed correctly in the course.')
    videos_viewed = IntegerField(description='Number of unique videos the learner has watched any part of in the'
                                             ' course.')
    discussion_contributions = IntegerField(description='Total number of posts, responses and comments the learner has'
                                                        ' made in the course.')
    segments = StringField(analyzed=True, description='Classifiers that help group learners by analyzing their activity'
                                                      ' patterns. Example: "inactive" indicates the user has not'
                                                      ' engaged with the course recently. This field is analyzed by'
                                                      ' elasticsearch so that searches can be made for learners that'
                                                      ' either have or don\'t have particular classifiers.')
    attempt_ratio_order = IntegerField(
        description='Used to sort learners by problem_attempts_per_completed in a meaningful way. When using'
                    ' problem_attempts_per_completed as your primary sort key, you can secondary sort by'
                    ' attempt_ratio_order to see struggling and high performing users. At one extreme this identifies'
                    ' users who have gotten many problems correct with the fewest number of attempts, at the other'
                    ' extreme it highlights users who have gotten very few (if any) problems correct with a very high'
                    ' number of attempts. The two extremes identify the highest performing and lowest performing'
                    ' learners according to this metric. To see high performing learners sort by'
                    ' (problem_attempts_per_completed ASC, attempt_ratio_order DESC). To see struggling learners sort'
                    ' by (problem_attempts_per_completed DESC, attempt_ratio_order ASC).'
    )
    # More user profile fields, appended after initial schema creation
    user_id = IntegerField(description='Learner\'s user ID.')
    language = StringField(description='Learner\'s preferred language.')
    location = StringField(description='Learner\'s reported location.')
    year_of_birth = IntegerField(description='Learner\'s reported year of birth.')
    level_of_education = StringField(description='Learner\'s reported level of education.')
    gender = StringField(description='Learner\'s reported gender.')
    mailing_address = StringField(description='Learner\'s reported mailing address.')
    city = StringField(description='Learner\'s reported city.')
    country = StringField(description='Learner\'s reported country.')
    goals = StringField(description='Learner\'s reported goals.')


class ModuleEngagementRosterTableTask(BareHiveTableTask):
    """A hive table for the roster data."""

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'module_engagement_roster'

    @property
    def columns(self):
        return ModuleEngagementRosterRecord.get_hive_schema()


class ModuleEngagementRosterPartitionTask(WeekIntervalMixin, ModuleEngagementDownstreamMixin, HivePartitionTask):
    """
    A Hive partition that represents the roster as of a particular day.

    Note that data from the prior 2 weeks is used to generate the summary for a particular date.
    """

    date = luigi.DateParameter()
    max_field_length = luigi.IntParameter(
        description='If set, truncate any long strings from the auth_userprofile table to this maximum length. '
                    ' This is required for ElasticSearch, which throws a MaxBytesLengthExceededException for any term '
                    ' that is longer than its configured max length.',
        default=20000,
    )

    interval = None
    partition_value = None

    def __init__(self, *args, **kwargs):
        super(ModuleEngagementRosterPartitionTask, self).__init__(*args, **kwargs)

        self.partition_value = self.date.isoformat()

    def query(self):
        # The end of the interval is not closed, so use the prior day's enrollment data.
        last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member

        def strip_and_truncate(field):
            """
            Identify delimiters in the data and strip them out to prevent parsing errors.

            Also, if self.max_field_length is set, then truncate the field to self.max_field_length.
            """
            stripped = "regexp_replace(regexp_replace({}, '\\\\t|\\\\n|\\\\r', ' '), '\\\\\\\\', '')".format(field)

            if self.max_field_length is not None:
                stripped = "substring({}, 1, {})".format(stripped, self.max_field_length)
            return stripped

        query = """
        USE {database_name};
        INSERT OVERWRITE TABLE {table} PARTITION ({partition.query_spec}) {if_not_exists}
        SELECT
            ce.course_id,
            au.username,
            '{start}',
            '{end}',
            au.email,
            {aup_name},
            ce.mode,
            lce.first_enrollment_date,
            cohort.name,
            COALESCE(eng.problem_attempts, 0),
            COALESCE(eng.problems_attempted, 0),
            COALESCE(eng.problems_completed, 0),
            eng.problem_attempts_per_completed,
            COALESCE(eng.videos_viewed, 0),
            COALESCE(eng.discussion_contributions, 0),
            CONCAT_WS(
                ",",
                IF(ce.at_end = 0, "unenrolled", NULL),
                -- The learner has had no activity in the past 2 weeks.
                IF(COALESCE(old_eng.days_active, 0) = 0 AND COALESCE(eng.days_active, 0) = 0, "inactive", NULL),
                -- Two weeks ago the learner was active, however, they have had no activity in the most recent week.
                IF(COALESCE(old_eng.days_active, 0) > 0 AND COALESCE(eng.days_active, 0) = 0, "disengaging", NULL),
                seg.segments
            ),
            -- attempt_ratio_order
            -- This field is a secondary sort key for the records in this table. Combined with a primary sort on
            -- problem_attempts_per_completed it can be used to identify top performers or struggling learners. It
            -- provides a magnitude for the degree to which a user is performing well or poorly. If, for example, they
            -- have made 10 attempts and gotten 10 problems correct, we want to sort that learner as higher
            -- performing than a user who has only made 1 attempt on one problem and was correct. Similarly, if a user
            -- has made 10 attempts without getting any problems correct, we want to sort that learner as lower
            -- performing than a user who has only made one attempt without getting the problem correct.

            -- To see high performing learners sort by (problem_attempts_per_completed ASC, attempt_ratio_order DESC)
            -- To see struggling learners sort by (problem_attempts_per_completed DESC, attempt_ratio_order ASC)
            IF(
                eng.problem_attempts_per_completed IS NULL,
                -COALESCE(eng.problem_attempts, 0),
                COALESCE(eng.problem_attempts, 0)
            ),
            aup.user_id,
            {aup_language},
            {aup_location},
            aup.year_of_birth,
            {aup_level_of_education},
            {aup_gender},
            {aup_mailing_address},
            {aup_city},
            {aup_country},
            {aup_goals}
        FROM course_enrollment ce
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
                MIN(`date`) AS first_enrollment_date
            FROM course_enrollment
            WHERE
                at_end = 1 AND `date` < '{end}'
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
            ce.`date` = '{last_complete_date}'
        """.format(
            start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
            end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
            last_complete_date=last_complete_date.isoformat(),
            partition=self.partition,
            if_not_exists='' if self.overwrite else 'IF NOT EXISTS',
            database_name=hive_database_name(),
            table=self.hive_table_task.table,
            aup_name=strip_and_truncate('aup.name'),
            aup_language=strip_and_truncate('aup.language'),
            aup_location=strip_and_truncate('aup.location'),
            aup_level_of_education=strip_and_truncate('aup.level_of_education'),
            aup_gender=strip_and_truncate('aup.gender'),
            aup_mailing_address=strip_and_truncate('aup.mailing_address'),
            aup_city=strip_and_truncate('aup.city'),
            aup_country=strip_and_truncate('aup.country'),
            aup_goals=strip_and_truncate('aup.goals'),
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
                overwrite_from_date=self.overwrite_from_date,
            ),
            ModuleEngagementSummaryPartitionTask(
                date=self.date,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
                overwrite_from_date=self.overwrite_from_date,
            ),
            ModuleEngagementSummaryPartitionTask(
                date=(self.date - datetime.timedelta(weeks=1)),
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
                overwrite_from_date=self.overwrite_from_date,
            ),
            ExternalCourseEnrollmentPartitionTask(
                interval_end=self.date
            ),
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportCourseUserGroupTask(**kwargs_for_db_import),
            ImportCourseUserGroupUsersTask(**kwargs_for_db_import),
            ImportAuthUserProfileTask(**kwargs_for_db_import),
        )


class ModuleEngagementRosterIndexDownstreamMixin(object):
    """Indexing parameters that can be specified at the workflow level."""

    obfuscate = luigi.BoolParameter(
        default=False,
        description='Generate fake names and email addresses for users. This can be used to generate production-like'
                    ' data sets that are more difficult to associate with particular users at a glance. Useful for'
                    ' testing.'
    )
    scale_factor = luigi.IntParameter(
        default=1,
        description='For each record, generate N more identical records with different IDs. This will result in a'
                    ' scaled up data set that can be used for performance testing the indexing and querying systems.'
    )
    indexing_tasks = luigi.IntParameter(
        default=None,
        significant=False,
        description=ElasticsearchIndexTask.indexing_tasks.description
    )


class ModuleEngagementRosterIndexTask(ModuleEngagementDownstreamMixin, ModuleEngagementRosterIndexDownstreamMixin,
                                      ElasticsearchIndexTask):
    """Load the roster data into elasticsearch for rapid query."""

    alias = luigi.Parameter(
        config_path={'section': 'module-engagement', 'name': 'alias'},
        description=ElasticsearchIndexTask.alias.description
    )
    number_of_shards = luigi.Parameter(
        config_path={'section': 'module-engagement', 'name': 'number_of_shards'},
        description=ElasticsearchIndexTask.number_of_shards.description
    )

    @property
    def partition_task(self):
        """The output from this task is indexed in elasticsearch."""
        return ModuleEngagementRosterPartitionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.other_reduce_tasks,
            overwrite=self.overwrite,
            date=self.date,
            overwrite_from_date=self.overwrite_from_date,
        )

    def requires_local(self):
        return self.partition_task

    def input_hadoop(self):
        return get_target_from_url(self.partition_task.partition_location)

    @property
    def properties(self):
        """Generate the elasticsearch mapping from the record schema."""
        return ModuleEngagementRosterRecord.get_elasticsearch_properties()

    def document_generator(self, lines):
        for line in lines:
            record = ModuleEngagementRosterRecord.from_tsv(line)

            if self.obfuscate:
                email = '{0}@example.com'.format(record.username)
                name = ' '.join([x.capitalize() for x in [random.choice(NAMES), random.choice(SURNAMES)]])
            else:
                email = record.email
                name = record.name

            document = {
                '_id': '|'.join([record.course_id, record.username]),
                '_source': {
                    'name': name,
                    'email': email
                }
            }

            for maybe_null_field in ModuleEngagementRosterRecord.get_fields():
                if maybe_null_field in ('name', 'email'):
                    continue
                maybe_null_value = getattr(record, maybe_null_field)
                if maybe_null_value is not None and maybe_null_value != float('inf'):
                    if maybe_null_field == 'segments':
                        maybe_null_value = maybe_null_value.split(',')
                    document['_source'][maybe_null_field] = maybe_null_value

            original_id = document['_id']
            for i in range(self.scale_factor):
                if i > 0:
                    document = document.copy()
                    document['_id'] = original_id + '|' + str(i)
                yield document


NAMES = ['james', 'john', 'robert', 'william', 'michael', 'david', 'richard', 'charles', 'joseph', 'thomas',
         'mary', 'patricia', 'linda', 'barbara', 'elizabeth', 'jennifer', 'maria', 'susan', 'margaret', 'dorothy']
SURNAMES = ['smith', 'johnson', 'williams', 'jones', 'brown', 'davis', 'miller', 'wilson', 'moore', 'taylor']


@workflow_entry_point  # pylint: disable=missing-docstring
class ModuleEngagementWorkflowTask(ModuleEngagementDownstreamMixin, ModuleEngagementRosterIndexDownstreamMixin,
                                   luigi.WrapperTask):
    __doc__ = """
    A rapidly searchable learner roster for each course with aggregate statistics about that learner's performance.

    Each record written to the elasticsearch index represents a single learner's performance in the course in the last
    week. Note that the index will contain records for users that have since unenrolled in the course, so there will
    be one record for every user who has ever been enrolled in that course.

    Given that each week the learner's statistics may have changed, the entire index is re-written every time this task
    is run. Elasticsearch does not support transactional updates. For this reason, we write to a separate index
    on each update, using an alias to point to the "live" index while we build the next version. Once the next version
    is complete, we switch over the alias to point to the newly built index and drop the old one. This allows us to
    continue to expose a consistent view of the roster while we are writing out a new version. Without an atomic toggle
    like this, it is possible an instructor could run a query and see a mix of data from the old and new versions of
    the index.

    For more information about this strategy see `Index Aliases and Zero Downtime`_.

    We organize the data in a single index that contains the data for all courses. We rely on the default
    elasticsearch sharding strategy instead of manually attempting to shard the data by course (or some other
    dimension). This choice was made largely because it is simpler to implement and manage.

    The index is optimized for the following access patterns within a single course:

    - Find learners by some part of their name. For example: "John" or "John Doe".
    - Find learners by their exact username. For example: "johndoe". Note that partial matches are not supported when
      searching by username. A query of "john" will not match the username "johndoe".
    - Find learners by their exact email address. For example: "johndoe@gmail.com".
    - Find learners by the segments they belong to. For example: "disengaging AND struggling". Note that these segments
      can be combined using arbitrary boolean logic "(disengaging AND struggling) OR highly_engaged".

    Each record contains the following fields: {record_doc}

    This workflow also generates two relational database tables in the result store:

    1. The `module_engagement` table which has one record per type of learner interaction with each module in the course
       in a given day. For example, if a learner clicked play on a video 4 times in a single day, there would be one
       record in this table that represented that interaction. It would contain the course, the date, the ID of the
       video, the learner's username, and the fact that they pressed play 4 times. Each record contains the following
       fields: {engagement_record_doc}

    2. The `module_engagement_metric_ranges` table which has one record per course metric range. These ranges specify
       various thresholds for bucketing user activity. Typically "low", "normal" or "high". Each record contains the
       following fields: {ranges_record_doc}

    .. _Index Aliases and Zero Downtime: https://www.elastic.co/guide/en/elasticsearch/guide/1.x/index-aliases.html
    """.format(
        record_doc=ModuleEngagementRosterRecord.get_restructured_text(),
        engagement_record_doc=ModuleEngagementRecord.get_restructured_text(),
        ranges_record_doc=ModuleEngagementSummaryMetricRangeRecord.get_restructured_text(),
    )

    overwrite_from_date = None
    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'module_engagement', 'name': 'overwrite_n_days'},
        significant=False,
        default=3,
        description='This parameter is passed down to the module engagement model which will overwrite data from a date'
                    ' in the past up to the end of the interval. Events are not always collected at the time that they'
                    ' are emitted, sometimes much later. By re-processing past days we can gather late events and'
                    ' include them in the computations. This parameter specifies the number of days to overwrite'
                    ' starting with the most recent date. A value of 0 indicates no days should be overwritten.'
    )

    # Don't use the OverwriteOutputMixin since it changes the behavior of complete() (which we don't want).
    overwrite = luigi.BoolParameter(default=False, significant=False)
    throttle = luigi.FloatParameter(
        config_path={'section': 'module-engagement', 'name': 'throttle'},
        description=ElasticsearchIndexTask.throttle.description,
        default=0.75,
        significant=False
    )
    host = luigi.Parameter(
        config_path={'section': 'elasticsearch', 'name': 'host'},
        description=ElasticsearchIndexTask.host.description,
        default='',
    )

    def requires(self):
        overwrite_from_date = self.date - datetime.timedelta(days=self.overwrite_n_days)

        # For clients that don't use Elasticsearch (or Insights), don't run ModuleEngagementRosterIndexTask.
        # Instead, just create the partition, so the data is available in hive.
        if self.host:
            yield ModuleEngagementRosterIndexTask(
                date=self.date,
                indexing_tasks=self.indexing_tasks,
                scale_factor=self.scale_factor,
                obfuscate=self.obfuscate,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite_from_date=overwrite_from_date,
                overwrite=self.overwrite,
                throttle=self.throttle
            )
        else:
            yield ModuleEngagementRosterPartitionTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                overwrite=self.overwrite,
                date=self.date,
                overwrite_from_date=overwrite_from_date,
            )

        yield ModuleEngagementSummaryMetricRangesMysqlTask(
            date=self.date,
            overwrite_from_date=overwrite_from_date,
            n_reduce_tasks=self.n_reduce_tasks,
        )

    def output(self):
        return [t.output() for t in self.requires()]
