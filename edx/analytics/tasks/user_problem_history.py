"""
Makes available the number of attempts, most recent score, and max
possible score for each course, user, and problem that the user
attempted in the course.
"""

import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.datetime_util import weekly_date_grouping_key
from edx.analytics.tasks.util.hive import HivePartition, HiveTableTask, MysqlInsertTask, WarehouseMixin


class UserProblemHistoryTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    Calculate problem history for each user in each course.
    """

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        event_time = event['time']

        if event.get('event_type') != 'problem_check' or event.get('event_source') != 'server':
            return  # We don't care about this type of event

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        problem_id = event_data.get('problem_id', None)
        if not problem_id:
            return

        date_grouping_key = weekly_date_grouping_key(_date_string, self.interval.date_b)

        user_id = event.get('context').get('user_id')
        if not user_id:
            return

        score = event_data.get('grade', None)
        if score is None:  # Can't use "if not score" check here because score might be 0
            return

        max_score = event_data.get('max_grade', None)
        if not max_score:
            return

        yield ((date_grouping_key, course_id, user_id, problem_id), (max_score, score, event_time))

    def reducer(self, key, attempts):
        """
        Calculate number of attempts and final score for events
        corresponding to user and course.
        """
        date_grouping_key, course_id, user_id, problem_id = key  # date_grouping_key == week_ending
        num_attempts = 0

        max_score = None
        event_time = None
        most_recent_score = 0

        for attempt in attempts:
            num_attempts += 1
            if not max_score:
                max_score = attempt[0]
            if not event_time or event_time < attempt[2]:
                event_time = attempt[2]
                most_recent_score = attempt[1]

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            date_grouping_key,
            course_id.encode('utf-8'),
            user_id,
            problem_id.encode('utf-8'),
            num_attempts,
            most_recent_score,
            max_score,
        )

    def output(self):
        return get_target_from_url(self.output_root)


class UserProblemHistoryMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the tasks below."""

    @property
    def partition(self):
        """ Hive Partition used to store the new data being generated """
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member


class UserProblemHistoryTableTask(UserProblemHistoryMixin, HiveTableTask):
    """ Hive table that stores the output of UserProblemHistoryTask. """

    @property
    def table(self):
        return 'user_problem_weekly_data'

    @property
    def columns(self):
        return [
            ('week_ending', 'STRING'),
            ('course_id', 'STRING'),
            ('user_id', 'INT'),
            ('problem_id', 'STRING'),
            ('num_attempts', 'INT'),
            ('most_recent_score', 'INT'),
            ('max_score', 'INT'),
        ]

    def requires(self):
        return UserProblemHistoryTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )

    def output(self):
        return get_target_from_url(self.partition_location)


class UserProblemHistoryToMySQLTask(UserProblemHistoryMixin, MysqlInsertTask):
    """ Transfer problem history from Hive To MySQL. """

    table = 'user_problem_weekly_data'

    @property
    def insert_source_task(self):
        return UserProblemHistoryTableTask(
            n_reduce_tasks=self.n_reduce_tasks,
            warehouse_path=self.warehouse_path,
            interval=self.interval
        )

    @property
    def columns(self):
        return [
            ('week_ending', 'DATE NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('user_id', 'INT NOT NULL'),
            ('problem_id', 'VARCHAR(255) NOT NULL'),
            ('num_attempts', 'INT NOT NULL'),
            ('most_recent_score', 'INT NOT NULL'),
            ('max_score', 'INT NOT NULL'),
        ]

    @property
    def default_columns(self):
        """ Columns and constraints that are managed by MySQL """
        return [
            ('created', 'TIMESTAMP DEFAULT NOW()'),
            ('CONSTRAINT week_course_user_problem', 'UNIQUE (week_ending, course_id, user_id, problem_id)')
        ]
