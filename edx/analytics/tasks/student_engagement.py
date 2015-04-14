"""Calculates per-student engagement reports per course."""

import csv
import logging
import datetime
import hashlib
from itertools import groupby
from operator import itemgetter
import re

import luigi

from edx.analytics.tasks.database_imports import (
    ImportAuthUserTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask)
from edx.analytics.tasks.enrollments import CourseEnrollmentTableTask
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util import Week
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin

from edx.analytics.tasks.util.hive import WarehouseMixin, HiveTableTask, HivePartition, HiveTableFromQueryTask

log = logging.getLogger(__name__)


class StudentEngagementTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    This is a spiked version of the basic task, used as a starting point for developing
    the workflow.
    """

    SUBSECTION_ACCESSED_PATTERN = r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware/[^/]+/[^/]+/.*$'

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
        elif event_type[:9] == '/courses/' and re.match(self.SUBSECTION_ACCESSED_PATTERN, event_type):
            timestamp = eventlog.get_event_time_string(event)
            if timestamp is None:
                return
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
        """Returns Week object corresponding to give date string."""
        split_date = date_string.split('-')
        date = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
        iso_year, iso_weekofyear, _iso_weekday = date.isocalendar()
        return Week(iso_year, iso_weekofyear)

    def reducer(self, key, events):
        """Calculate counts for events corresponding to user and course in a given time period."""
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
        for _entity_id, events in groupby(sorted_events, key=sort_key):
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


class JoinedStudentEngagementTableTask(StudentEngagementTableDownstreamMixin, HiveTableFromQueryTask):
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
        return """
        SELECT
            ce.date,
            ce.course_id,
            au.username,
            au.email,
            COALESCE(cohort.name, ''),
            COALESCE(ser.was_active, 0),
            COALESCE(ser.problems_attempted, 0),
            COALESCE(ser.problem_attempts, 0),
            COALESCE(ser.problems_correct, 0),
            COALESCE(ser.videos_played, 0),
            COALESCE(ser.forum_posts, 0),
            COALESCE(ser.forum_replies, 0),
            COALESCE(ser.forum_comments, 0),
            COALESCE(ser.textbook_pages_viewed, 0),
            COALESCE(ser.last_subsection_viewed, '')
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
        ) cohort
            ON (au.id = cohort.user_id AND ce.course_id = cohort.course_id)
        WHERE ce.at_end = 1 AND ce.date >= '{start}' AND ce.date < '{end}'
        """.format(
            start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
            end=self.interval.date_b.isoformat()  # pylint: disable=no-member
        )

    def requires(self):
        kwargs_for_db_import = {
            'overwrite': self.overwrite,
        }
        kwargs_for_engagement = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite': self.overwrite,
        }
        # For enrollment, use the default start date and the current
        # interval's end date to calculate. Note that if it's already
        # calculated, this won't check the interval that was used.
        kwargs_for_enrollment = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval_end': self.interval.date_b,  # pylint: disable=no-member
            'pattern': self.pattern,
            'overwrite': self.overwrite,
        }
        yield (
            StudentEngagementTableTask(**kwargs_for_engagement),
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportCourseUserGroupTask(**kwargs_for_db_import),
            ImportCourseUserGroupUsersTask(**kwargs_for_db_import),
            CourseEnrollmentTableTask(**kwargs_for_enrollment),
        )


class StudentEngagementDailyCsvFileTask(
        StudentEngagementTableDownstreamMixin,
        OverwriteOutputMixin,
        MultiOutputMapReduceJobTask):
    """
    Groups student engagement information by course, producing a different file for each.
    """

    def requires(self):
        return JoinedStudentEngagementTableTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            overwrite=self.overwrite,
        )

    def mapper(self, line):
        """
        Groups inputs by date and course_id, writes all records with the same course_id to the same output file.
        """
        # TSV's are assumed to be written (by Hive) in UTF-8 encoding,
        # so we do not have to encode the course_id before outputting.
        date, course_id, content = line.split('\t', 2)
        yield (date, course_id), content

    def output_path_for_key(self, key):
        """
        Match the course folder hierarchy that is expected by the instructor dashboard.

        The instructor dashboard expects the file to be stored in a
        folder named sha1(course_id).  All files in that directory
        will be displayed on the instructor dashboard for that course.
        """
        date, course_id = key
        hashed_course_id = hashlib.sha1(course_id).hexdigest()
        filename = u'student_engagement_daily_{date}.csv'.format(date=date)
        return url_path_join(self.output_root, hashed_course_id, filename)

    def get_column_names(self):
        """List names of columns as they should appear in the CSV, in order stored in Hive TSV output."""
        return [
            'date',
            'course_id',
            'username',
            'email',
            'cohort',
            'was_active',
            'problems_attempted',
            'problem_attempts',
            'problems_correct',
            'videos_played',
            'forum_posts',
            'forum_replies',
            'forum_comments',
            'textbook_pages_viewed',
            'last_subsection_viewed',
        ]

    def multi_output_reducer(self, key, values, output_file):
        """
        Each entry should be written to the output file in csv format.

        This output is visible to instructors, so use an excel friendly format (csv).
        """
        date, course_id = key
        field_names = self.get_column_names()

        writer = csv.DictWriter(output_file, field_names)
        writer.writerow(dict(
            (k, k) for k in field_names
        ))

        # Collect in memory the list of dicts to be output.  Then sort
        # the list of dicts by their field names before encoding.
        row_data = []
        for content in values:
            fields = content.split('\t')
            # skip the values from the key in field_names, and add values manually.
            row = {field_key: field_value for field_key, field_value in zip(field_names[2:], fields)}
            row['date'] = date
            row['course_id'] = course_id
            row_data.append(row)

        row_data = sorted(row_data, key=itemgetter(*field_names))

        for row_dict in row_data:
            # TSV's are assumed to be written (by Hive) in UTF-8 encoding,
            # so we should not encode the values of row_data before outputting.
            writer.writerow(row_dict)
