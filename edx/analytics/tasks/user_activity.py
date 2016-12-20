"""Categorize activity of users."""

import datetime
import logging
import textwrap
from collections import defaultdict

import luigi
import luigi.date_interval
from luigi.parameter import DateIntervalParameter

from edx.analytics.tasks.calendar_task import CalendarTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin, PathSelectionByDateIntervalTask
from edx.analytics.tasks.url import get_target_from_url, url_path_join, UncheckedExternalURL
import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask, BareHiveTableTask, HivePartitionTask, HiveQueryTask, hive_database_name
from edx.analytics.tasks.util.weekly_interval import WeeklyIntervalMixin
from edx.analytics.tasks.decorators import workflow_entry_point
from edx.analytics.tasks.mysql_load import MysqlInsertTask
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

log = logging.getLogger(__name__)

ACTIVE_LABEL = "ACTIVE"
PROBLEM_LABEL = "ATTEMPTED_PROBLEM"
PLAY_VIDEO_LABEL = "PLAYED_VIDEO"
POST_FORUM_LABEL = "POSTED_FORUM"


class UserActivityIntervalTask(
    OverwriteOutputMixin,
    WarehouseMixin,
    EventLogSelectionMixin,
    MultiOutputMapReduceJobTask):


    # FILEPATH_PATTERN should match the output files defined by output_path_for_key().
    FILEPATH_PATTERN = '.*?user_activity_(?P<date>\\d{4}-\\d{2}-\\d{2})'

    output_root = None

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        username = event.get('username', '').strip()
        if not username:
            return

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        for label in self.get_predicate_labels(event):
            yield date_string, self._encode_tuple((course_id, username, date_string, label))

    def get_predicate_labels(self, event):
        """Creates labels by applying hardcoded predicates to a single event."""
        # We only want the explicit event, not the implicit form.
        event_type = event.get('event_type')
        event_source = event.get('event_source')

        # Ignore all background task events, since they don't count as a form of activity.
        if event_source == 'task':
            return []

        # Ignore all enrollment events, since they don't count as a form of activity.
        if event_type.startswith('edx.course.enrollment.'):
            return []

        labels = [ACTIVE_LABEL]

        if event_source == 'server':
            if event_type == 'problem_check':
                labels.append(PROBLEM_LABEL)

            if event_type.startswith('edx.forum.') and event_type.endswith('.created'):
                labels.append(POST_FORUM_LABEL)

        if event_source in ('browser', 'mobile'):
            if event_type == 'play_video':
                labels.append(PLAY_VIDEO_LABEL)

        return labels

    def _encode_tuple(self, values):
        """
        Convert values into a tuple containing encoded strings.

        Parameters:
            Values is a list or tuple.

        This enforces a standard encoding for the parts of the
        key. Without this a part of the key might appear differently
        in the key string when it is coerced to a string by luigi. For
        example, if the same key value appears in two different
        records, one as a str() type and the other a unicode() then
        without this change they would appear as u'Foo' and 'Foo' in
        the final key string. Although python doesn't care about this
        difference, hadoop does, and will bucket the values
        separately. Which is not what we want.
        """
        # TODO: refactor this into a utility function and update jobs
        # to always UTF8 encode mapper keys.
        if len(values) > 1:
            return tuple([value.encode('utf8') for value in values])
        else:
            return values[0].encode('utf8')


    def multi_output_reducer(self, _date_string, values, output_file):
        frequency = defaultdict(int)

        for value in values:
            frequency[value] += 1

        for key in frequency.keys():
            num_events = frequency[key]
            course_id, username, date_string, label = key
            value = [course_id, username, date_string, label, num_events]
            output_file.write('\t'.join([str(field) for field in value]))
            output_file.write('\n')

    def output_path_for_key(self, key):
        date_string = key
        return url_path_join(
            self.hive_partition_path('user_activity', date_string),
            'user_activity_{date}'.format(
                date=date_string,
            ),
        )

    def downstream_input_tasks(self):
        tasks = []
        for date in self.interval:
            url = self.output_path_for_key(date.isoformat())
            tasks.append(UncheckedExternalURL(url))

        return tasks

    def run(self):
        self.remove_output_on_overwrite()
        super(UserActivityIntervalTask, self).run()

        for date in self.interval:
            url = self.output_path_for_key(date.isoformat())
            target = get_target_from_url(url)
            if not target.exists():
                target.open("w").close()  # touch the file


class UserActivityDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the UserActivityTableTask task."""
    pass



class UserActivityTask(UserActivityDownstreamMixin, OverwriteOutputMixin, luigi.WrapperTask):

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        significant=False,
    )

    def __init__(self, *args, **kwargs):
        super(UserActivityTask, self).__init__(*args, **kwargs)

        self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)

    def requires(self):
        overwrite_interval = DateIntervalParameter().parse('{}-{}'.format(
            self.overwrite_from_date,
            self.interval.date_b
        ))

        user_activity_interval_task =  UserActivityIntervalTask(
            interval=overwrite_interval,
            source=self.source,
            pattern=self.pattern,
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            overwrite=True,
        )

        path_selection_interval = DateIntervalParameter().parse('{}-{}'.format(
            self.interval.date_a,
            self.overwrite_from_date,
        ))
        user_activity_root = url_path_join(self.warehouse_path, 'user_activity')
        path_selection_task = PathSelectionByDateIntervalTask(
            source=[user_activity_root],
            interval=path_selection_interval,
            pattern=[UserActivityIntervalTask.FILEPATH_PATTERN],
            expand_interval=datetime.timedelta(0),
            date_pattern='%Y-%m-%d',
        )

        requirements = {
            'path_selection_task': path_selection_task,
            'user_activity_interval_task': user_activity_interval_task
        }

        #if self.overwrite_n_days > 0:
        #    requirements['downstream_input_tasks'] = self.requires_local().downstream_input_tasks()

        return requirements

    def output(self):
        tasks = [
            self.requires()['path_selection_task'],
            self.requires()['user_activity_interval_task'].downstream_input_tasks()
        ]
        
        return [task.output() for task tasks]


# class UserActivityTask(UserActivityDownstreamMixin, OverwriteOutputMixin, MapReduceJobTask):
#
#     output_root = None
#
#     enable_direct_output = True
#
#     overwrite_n_days = luigi.IntParameter(
#         config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
#         significant=False,
#     )
#
#     def __init__(self, *args, **kwargs):
#         super(UserActivityTask, self).__init__(*args, **kwargs)
#
#         self.overwrite_from_date = self.interval.date_b - datetime.timedelta(days=self.overwrite_n_days)
#
#
#     def requires_local(self):
#         if self.overwrite_n_days == 0:
#             return []
#
#         overwrite_interval = DateIntervalParameter().parse('{}-{}'.format(
#             self.overwrite_from_date,
#             self.interval.date_b
#         ))
#
#         return UserActivityIntervalTask(
#             interval=overwrite_interval,
#             source=self.source,
#             pattern=self.pattern,
#             n_reduce_tasks=self.n_reduce_tasks,
#             warehouse_path=self.warehouse_path,
#             overwrite=True,
#         )
#
#     def requires_hadoop(self):
#         path_selection_interval = DateIntervalParameter().parse('{}-{}'.format(
#             self.interval.date_a,
#             self.overwrite_from_date,
#         ))
#         user_activity_root = url_path_join(self.warehouse_path, 'user_activity')
#         path_selection_task = PathSelectionByDateIntervalTask(
#             source=[user_activity_root],
#             interval=path_selection_interval,
#             pattern=[UserActivityIntervalTask.FILEPATH_PATTERN],
#             expand_interval=datetime.timedelta(0),
#             date_pattern='%Y-%m-%d',
#         )
#
#         requirements = {
#             'path_selection_task': path_selection_task,
#         }
#
#         if self.overwrite_n_days > 0:
#             requirements['downstream_input_tasks'] = self.requires_local().downstream_input_tasks()
#
#         return requirements
#
#     def mapper(self, line):
#         (
#             course_id,
#             username,
#             date_string,
#             label,
#             num_events
#         ) = line.split('\t')
#         yield date_string, (course_id, username, date_string, label, num_events)
#
#     def reducer(self, key, values):
#         for value in values:
#             yield value
#
#     def output_url(self):
#         return self.hive_partition_path('user_activity_daily', self.interval.date_b)
#
#     def output(self):
#         return get_target_from_url(self.output_url())
#
#     def complete(self):
#         if self.overwrite and not self.attempted_removal:
#             return False
#         else:
#             return get_target_from_url(url_path_join(self.output_url(), '_SUCCESS')).exists()
#
#     def run(self):
#         self.remove_output_on_overwrite()
#         output_target = self.output()
#         # This is different from remove_output_on_overwrite()
#         # in that it also removes the target directory if
#         # the success marker file is missing.
#         if not self.complete() and output_target.exists():
#             output_target.remove()
#         super(UserActivityTask, self).run()


class UserActivityTableTask(BareHiveTableTask):

    @property
    def table(self):
        return 'user_activity_daily'

    @property
    def partition_by(self):
        return 'dt'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('date', 'STRING'),
            ('category', 'STRING'),
            ('count', 'INT'),
        ]


class UserActivityPartitionTask(UserActivityDownstreamMixin, HivePartitionTask):

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        significant=False,
    )

    @property
    def partition_value(self):
        return self.interval.date_b.isoformat()  # pylint: disable=no-member

    @property
    def hive_table_task(self):
        return UserActivityTableTask(
            warehouse_path=self.warehouse_path,
        )

    @property
    def data_task(self):
        return UserActivityTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            overwrite_n_days=self.overwrite_n_days
        )


class CourseActivityTask(OverwriteOutputMixin, UserActivityDownstreamMixin, HiveQueryTask):

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        significant=False,
    )

    def run(self):
        self.remove_output_on_overwrite()
        super(CourseActivityTask, self).run()

    def requires(self):
        yield (
            UserActivityPartitionTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                pattern=self.pattern,
                warehouse_path=self.warehouse_path,
                interval=self.interval,
                overwrite=self.overwrite,
                overwrite_n_days=self.overwrite_n_days,
            ),
            CalendarTableTask(
                warehouse_path=self.warehouse_path,
                overwrite=self.overwrite,
            )
        )


class CourseActivityWeeklyTask(CourseActivityTask):

    def query(self):

        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                course_id STRING,
                interval_start TIMESTAMP,
                interval_end TIMESTAMP,
                label STRING,
                count INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                act.course_id as course_id,
                CONCAT(cal.iso_week_start, ' 00:00:00') as interval_start,
                CONCAT(cal.iso_week_end, ' 00:00:00') as interval_end,
                act.category as label,
                COUNT(DISTINCT username) as count
            FROM user_activity_daily act
            JOIN calendar cal ON act.date = cal.date
            WHERE "{interval_start}" <= cal.date AND cal.date < "{interval_end}"
            GROUP BY
                act.course_id,
                cal.iso_week_start,
                cal.iso_week_end,
                act.category;
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.output().path,
            table_name='course_activity',
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat()
        )
        log.debug('Executing hive query: %s', query)
        return query

    def output(self):
        return get_target_from_url(
            url_path_join(self.warehouse_path, 'course_activity/')
        )


class CourseActivityDailyTask(CourseActivityTask):

    def query(self):

        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                date STRING,
                course_id STRING,
                label STRING,
                count INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                act.date,
                act.course_id as course_id,
                act.category as label,
                COUNT(DISTINCT username) as count
            FROM user_activity_daily act
            WHERE "{interval_start}" <= act.date AND act.date < "{interval_end}"
            GROUP BY
                act.course_id,
                act.date,
                act.category;
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.output().path,
            table_name='course_activity_daily',
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat()
        )
        log.debug('Executing hive query: %s', query)
        return query

    def output(self):
        return get_target_from_url(
            url_path_join(self.warehouse_path, 'course_activity_daily/')
        )


class CourseActivityMonthlyTask(CourseActivityTask):

    end_date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='A date within the month that will be the upper bound of the closed-open interval. '
        'Default is today, UTC.',
    )
    months = luigi.IntParameter(
        default=6,
        description='The number of months to include in the analysis, counting back from the month that contains '
        'the end_date.',
    )

    @property
    def interval(self):
        """Given the parameters, compute the first and last date of the interval."""
        from dateutil.relativedelta import relativedelta

        # We don't actually care about the particular day of the month in this computation since we are fixing both the
        # start and end dates to the first day of the month, so we can perform simple arithmetic with the numeric month
        # and only have to worry about adjusting the year. Note that bankers perform this arithmetic differently so it
        # is spelled out here explicitly even though their are third party libraries that contain this computation.

        if self.months == 0:
            raise ValueError('Number of months to process must be greater than 0')

        ending_date = self.end_date.replace(day=1)  # pylint: disable=no-member
        starting_date = ending_date - relativedelta(months=self.months)

        return luigi.date_interval.Custom(starting_date, ending_date)

    def query(self):

        query_format = textwrap.dedent("""
            USE {database_name};
            DROP TABLE IF EXISTS {table_name};
            CREATE EXTERNAL TABLE {table_name} (
                course_id STRING,
                year INT,
                month INT,
                label STRING,
                count INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION '{location}';

            INSERT OVERWRITE TABLE {table_name}
            SELECT
                act.course_id as course_id,
                cal.year,
                cal.month,
                act.category as label,
                COUNT(DISTINCT username) as count
            FROM user_activity_daily act
            JOIN calendar cal ON act.date = cal.date
            WHERE "{interval_start}" <= cal.date AND cal.date < "{interval_end}"
            GROUP BY
                act.course_id,
                cal.year,
                cal.month,
                act.category;
        """)

        query = query_format.format(
            database_name=hive_database_name(),
            location=self.output().path,
            table_name='course_activity_monthly',
            interval_start=self.interval.date_a.isoformat(),
            interval_end=self.interval.date_b.isoformat()
        )
        log.debug('Executing hive query: %s', query)
        return query

    def output(self):
        return get_target_from_url(
            url_path_join(self.warehouse_path, 'course_activity_monthly/')
        )

class InsertToMysqlCourseActivityTask(WeeklyIntervalMixin, UserActivityDownstreamMixin, MysqlInsertTask):

    overwrite_n_days = luigi.IntParameter(
        config_path={'section': 'user-activity', 'name': 'overwrite_n_days'},
        significant=False,
    )

    @property
    def table(self):
        return "course_activity"

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('interval_start', 'DATETIME NOT NULL'),
            ('interval_end', 'DATETIME NOT NULL'),
            ('label', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'label'),
            ('interval_end',)
        ]

    @property
    def insert_source_task(self):
        return CourseActivityWeeklyTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval=self.interval,
            overwrite=self.overwrite,
            overwrite_n_days=self.overwrite_n_days,
        )


class InsertToMysqlCourseActivityDailyTask(UserActivityDownstreamMixin, MysqlInsertTask):

    @property
    def table(self):
        return "course_activity_daily"

    @property
    def columns(self):
        return [
            ('date', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('label', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'label'),
            ('date',)
        ]

    @property
    def insert_source_task(self):
        return CourseActivityDailyTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval=self.interval,
            overwrite=self.overwrite,
        )


class InsertToMysqlCourseActivityMonthlyTask(UserActivityDownstreamMixin, MysqlInsertTask):

    @property
    def table(self):
        return 'course_activity_monthly'

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('year', 'INT(11) NOT NULL'),
            ('month', 'INT(11) NOT NULL'),
            ('label', 'VARCHAR(255) NOT NULL'),
            ('count', 'INT(11) NOT NULL'),
        ]

    @property
    def indexes(self):
        return [
            ('course_id', 'label'),
            ('year', 'month')
        ]

    @property
    def insert_source_task(self):
        return CourseActivityMonthlyTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval=self.interval,
            overwrite=self.overwrite,
        )
