""" Calculates problem history for individual users and stores it in Analytics DB. """

import datetime
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import HivePartition, HiveQueryToMysqlTask, HiveTableTask, WarehouseMixin


PROBLEM_CHECK = 'problem_check'

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

        if event.get('event_type') != PROBLEM_CHECK or event.get('event_source') != 'server':
            return  # We don't care about this type of event

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        username = event.get('username', None).strip()
        if not username:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        problem_id = event_data.get('problem_id', None)
        if not problem_id:
            return

        date_grouping_key = _date_string

        last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
        last_weekday = last_complete_date.isoweekday()

        split_date = _date_string.split('-')
        event_date = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        event_weekday = event_date.isoweekday()

        days_until_end = last_weekday - event_weekday
        if days_until_end < 0:
            days_until_end += 7

        end_of_week_date = event_date + datetime.timedelta(days=days_until_end)
        date_grouping_key = end_of_week_date.isoformat()

        user_id = event.get('context').get('user_id')
        if not user_id:
            return

        grade = event_data.get('grade', None)
        if grade is None:  # Can't use "if not grade" check here because grade might be 0
            return

        max_grade = event_data.get('max_grade', None)
        if not max_grade:
            return

        yield ((date_grouping_key, course_id, username, problem_id), (user_id, max_grade, grade, event_time))

    def reducer(self, key, attempts):
        """
        Calculate number of attempts and final score for events
        corresponding to user and course.
        """
        date_grouping_key, course_id, username, problem_id = key  # date_grouping_key == week_ending
        attempts = list(attempts)
        num_attempts = len(attempts)

        user_id = None
        max_grade = None
        event_time = None
        most_recent_grade = 0

        for attempt in attempts:
            if not user_id:
                user_id = attempt[0]
            if not max_grade:
                max_grade = attempt[1]
            if not event_time or event_time < attempt[3]:
                event_time = attempt[3]
                most_recent_grade = attempt[2]

        final_score = '{}/{}'.format(most_recent_grade, max_grade)

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            date_grouping_key,
            course_id.encode('utf-8'),
            user_id,
            problem_id.encode('utf-8'),
            num_attempts,
            final_score.encode('utf-8'),
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
            ('user_id', 'STRING'),
            ('problem_id', 'STRING'),
            ('num_attempts', 'INT'),
            ('final_score', 'STRING'),
        ]

    def requires(self):
        return UserProblemHistoryTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class UserProblemHistoryToMySQLTask(UserProblemHistoryMixin, HiveQueryToMysqlTask):
    """ Transfer problem history from Hive To MySQL. """

    @property
    def table(self):
        return 'user_problem_weekly_data'

    @property
    def query(self):
        return """
            SELECT
                week_ending,
                course_id,
                user_id,
                problem_id,
                num_attempts,
                final_score
            FROM user_problem_weekly_data up
            WHERE up.dt >= '{start_date}' AND up.dt <= '{end_date}'
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
        )

    @property
    def columns(self):
        return [
            ('week_ending', 'VARCHAR(30) NOT NULL'),
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('user_id', 'INT NOT NULL'),
            ('problem_id', 'VARCHAR(255) NOT NULL'),
            ('num_attempts', 'INT NOT NULL'),
            ('final_score', 'VARCHAR(20) NOT NULL'),
        ]

    @property
    def required_table_tasks(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            UserProblemHistoryTableTask(**kwargs),
        )
