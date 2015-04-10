
import logging
import datetime
from itertools import groupby
from operator import itemgetter
import re
import gzip
import textwrap

import luigi

from edx.analytics.tasks.database_imports import ImportAuthUserTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask
from edx.analytics.tasks.enrollments import CourseEnrollmentTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util import Week

from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveQueryToMysqlTask

log = logging.getLogger(__name__)


class StudentEngagementTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    This is a spiked version of the basic task, used as a starting point for developing 
    the workflow.
    """

    output_root = luigi.Parameter()
    group_by_week = luigi.BooleanParameter(default=False)

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

        timestamp = eventlog.get_event_time_string(event)
        if timestamp is None:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        event_source = event.get('event_source')

        entity_id = ''
        info = {}
        if event_type == 'problem_check':
            if event_source != 'server':
                return

            problem_id = event_data.get('problem_id')
            if not problem_id:
                return

            entity_id = problem_id
            if event_data.get('success', 'incorrect').lower() == 'correct':
                info['correct'] = True
        elif event_type == 'play_video':
            encoded_module_id = event_data.get('id')
            if not encoded_module_id:
                return

            entity_id = encoded_module_id
        elif event_type[:9] == '/courses/' and re.match(r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware/[^/]+/[^/]+/\d*$', event_type):
            info['path'] = event_type
            info['timestamp'] = timestamp
            event_type = 'marker:last_subsection_viewed'

        date_grouping_key = date_string
        if self.group_by_week:
            week_of_year = self.get_iso_week_containing_date(date_string)
            start_date = week_of_year.monday()
            end_date = week_of_year.sunday() + datetime.timedelta(days=1)
            date_grouping_key = '{0}-{1}'.format(start_date.isoformat(), end_date.isoformat())

        yield ((date_grouping_key, course_id, username), (entity_id, event_type, info))

    def get_iso_week_containing_date(self, date_string):
        split_date = date_string.split('-')
        date = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        iso_year, iso_weekofyear, iso_weekday = date.isocalendar()
        return Week(iso_year, iso_weekofyear)

    def reducer(self, key, events):
        date_grouping_key, course_id, username = key
        sort_key = itemgetter(0)
        sorted_events = sorted(events, key=sort_key)

        was_active = 1
        num_problems_attempted = 0
        num_problem_attempts = 0
        num_problems_correct = 0
        num_videos_played = 0
        num_forum_comments = 0
        num_forum_replies = 0
        num_forum_posts = 0
        num_textbook_pages = 0
        max_timestamp = None
        last_subsection_viewed = ''
        for entity_id, events in groupby(sorted_events, key=sort_key):
            is_first = True
            is_correct = False

            for _, event_type, info in events:
                if event_type == 'problem_check':
                    if is_first:
                        num_problems_attempted += 1
                    num_problem_attempts += 1
                    if not is_correct and info.get('correct', False):
                        is_correct = True
                elif event_type == 'play_video':
                    if is_first:
                        num_videos_played += 1
                elif event_type == 'edx.forum.comment.created':
                    num_forum_comments += 1
                elif event_type == 'edx.forum.response.created':
                    num_forum_replies += 1
                elif event_type == 'edx.forum.thread.created':
                    num_forum_posts += 1
                elif event_type == 'book':
                    num_textbook_pages += 1
                elif event_type == 'marker:last_subsection_viewed':
                    if not max_timestamp or info['timestamp'] > max_timestamp:
                        last_subsection_viewed = info['path']
                        max_timestamp = info['timestamp']

                if is_first:
                    is_first = False

            if is_correct:
                num_problems_correct += 1

        yield (
            date_grouping_key,
            course_id,
            username,
            was_active,
            num_problems_attempted,
            num_problem_attempts,
            num_problems_correct,
            num_videos_played,
            num_forum_posts,
            num_forum_replies,
            num_forum_comments,
            num_textbook_pages,
            last_subsection_viewed
        )

    def output(self):
        return get_target_from_url(self.output_root)


# After generating the output, need to join with auth_user to get the
# email (joining on username), and would also need to get the user_id
# to be able to join with enrollment data.

# In order to be able to perform this join, we first need to import this
# data into Hive.  Then define the other tables as also being in Hive.

class StudentEngagementTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the StudentEngagementTableTask task."""
    pass


class StudentEngagementTableTask(StudentEngagementTableDownstreamMixin, HiveTableTask):
    """Hive table that stores the set of students engaged in each course over time."""

    @property
    def table(self):
        return 'student_engagement_raw'

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('was_active', 'TINYINT'),
            ('problems_attempted', 'INT'),
            ('problem_attempts', 'INT'),
            ('problems_correct', 'INT'),
            ('videos_played', 'INT'),
            ('forum_posts', 'INT'),
            ('forum_replies', 'INT'),
            ('forum_comments', 'INT'),
            ('textbook_pages_viewed', 'INT'),
            ('last_subsection_viewed', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return StudentEngagementTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class MyHiveTableFromQueryTask(HiveTableTask):  # pylint: disable=abstract-method
    """Creates a hive table from the results of a hive query."""

    # insert_query = luigi.Parameter()
    # table = luigi.Parameter()
    # columns = luigi.Parameter(is_list=True)
    # partition = HivePartitionParameter()

    def query(self):
        create_table_statements = super(MyHiveTableFromQueryTask, self).query()
        full_insert_query = """
            INSERT INTO TABLE {table}
            PARTITION ({partition.query_spec})
            {insert_query}
        """.format(
            table=self.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),  # pylint: disable=no-member
        )
        return create_table_statements + textwrap.dedent(full_insert_query)

    def output(self):
        return get_target_from_url(self.partition_location)


class AllStudentEngagementTableTask(StudentEngagementTableDownstreamMixin, MyHiveTableFromQueryTask):
    """
    Join additional information onto raw student engagement data, but leave information in Hive,
    not in Mysql.

    Just need cohort and email, and to add (zeroed) entries for enrolled users who were not among the active.

    Doesn't look like the base class is ever used, and not sure it's right.  So pulling in a copy
    to work on instead.
    """
    
    @property
    def table(self):
        return 'student_engagement_joined'

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def columns(self):
        return [
            ('date', 'STRING'),
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('email', 'STRING'),
            ('cohort', 'STRING'),
            ('was_active', 'TINYINT'),
            ('problems_attempted', 'INT'),
            ('problem_attempts', 'INT'),
            ('problems_correct', 'INT'),
            ('videos_played', 'INT'),
            ('forum_posts', 'INT'),
            ('forum_replies', 'INT'),
            ('forum_comments', 'INT'),
            ('textbook_pages_viewed', 'INT'),
            ('last_subsection_viewed', 'STRING'),
        ]

    @property
    def insert_query(self):
        # Note that the inner joins are wrong to be using on the cohort identification,
        # because not every user_id will have a cohort assignment.  Using inner joins
        # will limit all engagement to cohorted users (and the courses in which they are cohorted).
        # So those should be left joins.  But we are doing an inner join with auth_user
        # in order to even get an id.  So we may need a subquery.
        return """
        SELECT ce.date, ce.course_id, au.username, au.email,
            c.name,
            ser.was_active, ser.problems_attempted, ser.problem_attempts, ser.problems_correct,
            ser.videos_played, ser.forum_posts, ser.forum_replies, ser.forum_comments,
            ser.textbook_pages_viewed, ser.last_subsection_viewed
        FROM course_enrollment ce
        INNER JOIN auth_user au
            ON (ce.user_id = au.id)
        LEFT OUTER JOIN student_engagement_raw ser
            ON (au.username = ser.username AND ce.date = ser.date and ce.course_id = ser.course_id)
        LEFT OUTER JOIN (
            SELECT
                cugu.user_id,
                cug.course_id,
                cug.name
            FROM course_groups_courseusergroup_users cugu
            INNER JOIN course_groups_courseusergroup cug
                ON (cugu.courseusergroup_id = cug.id)
        ) AS c
            ON (au.id = c.user_id AND ce.course_id = c.course_id)
        LEFT OUTER JOIN course_groups_courseusergroup_users cugu
            ON (au.id = cugu.user_id)
        LEFT OUTER JOIN course_groups_courseusergroup cug
            ON (cugu.courseusergroup_id = cug.id AND ce.course_id = cug.course_id)
        WHERE ce.at_end = 1 AND ce.date >= '{start}' AND ce.date < '{end}'

        """.format(
            start=self.interval.date_a.isoformat(),
            end=self.interval.date_b.isoformat()
        )

    def requires(self):
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        kwargs_for_logfiles = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite': self.overwrite,
        }
        yield (
            StudentEngagementTableTask(**kwargs_for_logfiles),
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportCourseUserGroupTask(**kwargs_for_db_import),
            ImportCourseUserGroupUsersTask(**kwargs_for_db_import),
            CourseEnrollmentTableTask(**kwargs_for_logfiles),
        )
