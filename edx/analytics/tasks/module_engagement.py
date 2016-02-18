"""Measure student engagement with individual modules in the course"""

import logging

import luigi
import luigi.task
from luigi import date_interval
from luigi.configuration import get_config

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.vertica_load import VerticaCopyTask
from edx.analytics.tasks.mysql_load import IncrementalMysqlInsertTask

from edx.analytics.tasks.util.hive import (
    WarehouseMixin, BareHiveTableTask, HivePartitionTask
)
from edx.analytics.tasks.util.record import Record, StringField, IntegerField, DateField

log = logging.getLogger(__name__)


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
            user_actions.append('played')
            entity_id = event_data.get('id')
        elif event_type.startswith('edx.forum.'):
            entity_type = 'forum'
            if event_type.endswith('.created'):
                if event_type == 'edx.forum.comment.created':
                    user_actions.append('commented')
                elif event_type == 'edx.forum.response.created':
                    user_actions.append('responded')
                elif event_type == 'edx.forum.thread.created':
                    user_actions.append('created')

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

    def requires(self):
        yield self.data_task
        yield self.hive_table_task

    @property
    def data_task(self):
        """The task that generates the raw data inside this hive partition."""
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
