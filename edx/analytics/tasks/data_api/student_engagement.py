"""Calculates per-student engagement reports per course."""

import csv
import datetime
import hashlib
import json
import logging
import re
import textwrap
from itertools import groupby
from operator import itemgetter

import luigi
from luigi.contrib.hive import HiveQueryTask

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.mysql_load import MysqlInsertTask
from edx.analytics.tasks.common.pathutil import EventLogSelectionDownstreamMixin, EventLogSelectionMixin
from edx.analytics.tasks.insights.calendar_task import CalendarTableTask
from edx.analytics.tasks.insights.database_imports import (
    ImportAuthUserTask, ImportCourseUserGroupTask, ImportCourseUserGroupUsersTask
)
from edx.analytics.tasks.insights.enrollments import CourseEnrollmentPartitionTask
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import (
    BareHiveTableTask, HivePartition, HivePartitionTask, HiveTableFromQueryTask, HiveTableTask, WarehouseMixin,
    hive_database_name
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.record import DateField, IntegerField, Record, StringField
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)

SUBSECTION_VIEWED_MARKER = 'marker:last_subsection_viewed'


class StudentEngagementIntervalTypeRecord(Record):
    """
    Student Engagement information used to populate student_engagement_{interval_type} tables.
    """

    end_date = DateField(description='End date of the interval being analyzed.')
    course_id = StringField(nullable=False, length=255, description='Identifier of course run.')
    username = StringField(
        nullable=False,
        length=255,
        description='The username of the user who was logged in when the event was emitted.'
    )
    days_active = IntegerField(description='Count of days user has been active during the interval.')
    problems_attempted = IntegerField(description='Count of unique problems attempted.')
    problem_attempts = IntegerField(description='Total count of problem attempts.')
    problems_correct = IntegerField(description='Count of unique problems that were answered correctly.')
    videos_played = IntegerField(description='Count of unique videos played.')
    forum_posts = IntegerField(description='Count of discussion posts.')
    forum_responses = IntegerField(description='Count of discussion responses created by user.')
    forum_comments = IntegerField(description='Count of discussion comments by user.')
    forum_upvotes_given = IntegerField(description='Total upvotes given by user on discussion posts.')
    forum_downvotes_given = IntegerField(description='Total downvotes given by user on discussion posts.')
    forum_upvotes_received = IntegerField(description='Total upvotes received by user on discussion posts.')
    forum_downvotes_received = IntegerField(description='Total downvotes received by user on discussion posts.')
    textbook_pages_viewed = IntegerField(description='Total textbook pages viewed by user.')
    last_subsection_viewed = StringField(
        nullable=False,
        length=1000,
        description='Page URL which was last visited by user during the interval.'
    )


class StudentEngagementTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    Calculate student engagement for a given interval and interval type.

    Calculates separately for each user in each course.
    """

    SUBSECTION_ACCESSED_PATTERN = r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware/[^/]+/[^/]+/.*$'

    output_root = luigi.Parameter()
    interval_type = luigi.Parameter(default="daily")

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
        forum_post_voted = None
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
            event_type = SUBSECTION_VIEWED_MARKER
        elif event_type.startswith('edx.forum'):
            forum_post_voted = re.match(r'edx\.forum\.(?P<post_type>\w+)\.voted', event_type)
            if forum_post_voted:
                info['vote_value'] = event_data.get('vote_value')
                if info['vote_value'] not in ['up', 'down']:
                    return
                info['undo_vote'] = event_data.get('undo_vote', False)

        date_grouping_key = date_string

        if self.interval_type == 'weekly':
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            last_weekday = last_complete_date.isoweekday()

            split_date = date_string.split('-')
            event_date = datetime.date(int(split_date[0]), int(split_date[1]), int(split_date[2]))
            event_weekday = event_date.isoweekday()

            days_until_end = last_weekday - event_weekday
            if days_until_end < 0:
                days_until_end += 7

            end_of_week_date = event_date + datetime.timedelta(days=days_until_end)
            date_grouping_key = end_of_week_date.isoformat()

        elif self.interval_type == 'all':
            # If gathering all data for a given user, use the last complete day of the interval
            # for joining with enrollment.
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            date_grouping_key = last_complete_date.isoformat()

        yield ((date_grouping_key, course_id, username), (entity_id, event_type, json.dumps(info), date_string))

        if forum_post_voted:
            # We emit two events for each "voted" event - one for the voting user and one for the
            # user receiving the vote.
            username = event_data.get('target_username')
            if not username:
                return
            event_type = 'edx.forum.{}.vote_received'.format(forum_post_voted.group('post_type'))
            yield ((date_grouping_key, course_id, username), (entity_id, event_type, json.dumps(info), date_string))

    def reducer(self, key, events):
        """Calculate counts for events corresponding to user and course in a given time period."""
        date_grouping_key, course_id, username = key

        sort_key = itemgetter(0)
        sorted_events = sorted(events, key=sort_key)
        if len(sorted_events) == 0:
            return

        num_problems_attempted = 0
        num_problem_attempts = 0
        num_problems_correct = 0
        num_videos_played = 0
        num_forum_comments = 0
        num_forum_responses = 0
        num_forum_posts = 0
        num_forum_upvotes_given = 0
        num_forum_downvotes_given = 0
        num_forum_upvotes_received = 0
        num_forum_downvotes_received = 0
        num_textbook_pages = 0
        dates_active = set()
        max_timestamp = None
        last_subsection_viewed = ''
        for _entity_id, events in groupby(sorted_events, key=sort_key):
            is_first = True
            is_correct = False

            for _, event_type, info_json, date_string in events:
                info = json.loads(info_json)
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
                    num_forum_responses += 1
                elif event_type == 'edx.forum.thread.created':
                    num_forum_posts += 1
                elif event_type in ('edx.forum.response.voted', 'edx.forum.thread.voted'):
                    vote_count = 1
                    if info['undo_vote']:
                        vote_count = -1
                    if info['vote_value'] == 'up':
                        num_forum_upvotes_given += vote_count
                    else:
                        num_forum_downvotes_given += vote_count
                elif event_type in ('edx.forum.response.vote_received', 'edx.forum.thread.vote_received'):
                    vote_count = [1, -1][info['undo_vote']]
                    if info['vote_value'] == 'up':
                        num_forum_upvotes_received += vote_count
                    else:
                        num_forum_downvotes_received += vote_count
                elif event_type == 'book':
                    num_textbook_pages += 1
                elif event_type == SUBSECTION_VIEWED_MARKER:
                    if not max_timestamp or info['timestamp'] > max_timestamp:
                        last_subsection_viewed = info['path']
                        max_timestamp = info['timestamp']

                if is_first:
                    is_first = False

                if date_string not in dates_active:
                    dates_active.add(date_string)

            if is_correct:
                num_problems_correct += 1

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            date_grouping_key,
            course_id.encode('utf-8'),
            username.encode('utf-8'),
            len(dates_active),
            num_problems_attempted,
            num_problem_attempts,
            num_problems_correct,
            num_videos_played,
            num_forum_posts,
            num_forum_responses,
            num_forum_comments,
            num_forum_upvotes_given,
            num_forum_downvotes_given,
            num_forum_upvotes_received,
            num_forum_downvotes_received,
            num_textbook_pages,
            last_subsection_viewed.encode('utf-8'),
        )

    def output(self):
        return get_target_from_url(self.output_root)


# After generating the output, need to join with auth_user to get the
# email (joining on username), and would also need to get the user_id
# to be able to join with enrollment data.

# In order to be able to perform this join, we first need to import this
# data into Hive.  Then define the other tables as also being in Hive.

class StudentEngagementTableDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the StudentEngagementRawTableTask task."""

    interval_type = luigi.Parameter(default="daily")


class StudentEngagementRawTableTask(StudentEngagementTableDownstreamMixin, HiveTableTask):
    """Hive table that stores the set of students engaged in each course over time."""

    @property
    def table(self):
        return 'student_engagement_raw_{}'.format(self.interval_type)

    @property
    def columns(self):
        return StudentEngagementIntervalTypeRecord.get_hive_schema()

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
            interval_type=self.interval_type,
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
        return 'student_engagement_joined_{}'.format(self.interval_type)

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    @property
    def columns(self):
        return [
            ('end_date', 'STRING'),
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('email', 'STRING'),
            ('cohort', 'STRING'),
            ('days_active', 'INT'),
            ('problems_attempted', 'INT'),
            ('problem_attempts', 'INT'),
            ('problems_correct', 'INT'),
            ('videos_played', 'INT'),
            ('forum_posts', 'INT'),
            ('forum_responses', 'INT'),
            ('forum_comments', 'INT'),
            ('forum_upvotes_given', 'INT'),
            ('forum_downvotes_given', 'INT'),
            ('forum_upvotes_received', 'INT'),
            ('forum_downvotes_received', 'INT'),
            ('textbook_pages_viewed', 'INT'),
            ('last_subsection_viewed', 'STRING'),
        ]

    @property
    def insert_query(self):
        # Join with calendar data only if calculating weekly engagement.
        calendar_join = ""
        if self.interval_type == "daily":
            date_where = "ce.`date` >= '{start}' AND ce.`date` < '{end}'".format(
                start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
                end=self.interval.date_b.isoformat()  # pylint: disable=no-member
            )
        elif self.interval_type == "weekly":
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            iso_weekday = last_complete_date.isoweekday()
            calendar_join = "INNER JOIN calendar cal ON (ce.`date` = cal.`date`) "
            date_where = "ce.`date` >= '{start}' AND ce.`date` < '{end}' AND cal.iso_weekday = {iso_weekday}".format(
                start=self.interval.date_a.isoformat(),  # pylint: disable=no-member
                end=self.interval.date_b.isoformat(),  # pylint: disable=no-member
                iso_weekday=iso_weekday,
            )
        elif self.interval_type == "all":
            last_complete_date = self.interval.date_b - datetime.timedelta(days=1)  # pylint: disable=no-member
            date_where = "ce.`date` = '{last_complete_date}'".format(last_complete_date=last_complete_date.isoformat())

        return """
        SELECT
            ce.`date`,
            ce.course_id,
            au.username,
            au.email,
            COALESCE(cohort.name, ''),
            COALESCE(ser.days_active, 0),
            COALESCE(ser.problems_attempted, 0),
            COALESCE(ser.problem_attempts, 0),
            COALESCE(ser.problems_correct, 0),
            COALESCE(ser.videos_played, 0),
            COALESCE(ser.forum_posts, 0),
            COALESCE(ser.forum_responses, 0),
            COALESCE(ser.forum_comments, 0),
            COALESCE(ser.forum_upvotes_given, 0),
            COALESCE(ser.forum_downvotes_given, 0),
            COALESCE(ser.forum_upvotes_received, 0),
            COALESCE(ser.forum_downvotes_received, 0),
            COALESCE(ser.textbook_pages_viewed, 0),
            COALESCE(ser.last_subsection_viewed, '')
        FROM course_enrollment ce
        {calendar_join}
        INNER JOIN auth_user au
            ON (ce.user_id = au.id)
        LEFT OUTER JOIN student_engagement_raw_{interval_type} ser
            ON (au.username = ser.username AND ce.`date` = ser.end_date and ce.course_id = ser.course_id)
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
        WHERE ce.at_end = 1 AND {date_where}
        """.format(
            calendar_join=calendar_join,
            interval_type=self.interval_type,
            date_where=date_where
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
            'interval_type': self.interval_type,
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
            StudentEngagementRawTableTask(**kwargs_for_engagement),
            ImportAuthUserTask(**kwargs_for_db_import),
            ImportCourseUserGroupTask(**kwargs_for_db_import),
            ImportCourseUserGroupUsersTask(**kwargs_for_db_import),
            CourseEnrollmentPartitionTask(**kwargs_for_enrollment),
        )
        # Only the weekly requires use of the calendar.
        if self.interval_type == "weekly":
            yield (
                CalendarTableTask(
                    warehouse_path=self.warehouse_path,
                )
            )


class StudentEngagementCsvFileTask(
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
            interval_type=self.interval_type,
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
        if self.interval_type == "all":
            date = str(self.interval)
        hashed_course_id = hashlib.sha1(course_id).hexdigest()
        filename = u'student_engagement_{interval_type}_{date}.csv'.format(date=date, interval_type=self.interval_type)
        return url_path_join(self.output_root, hashed_course_id, filename)

    def _get_date_header(self):
        """Gets column header for date, conditional on interval type."""
        return 'Date' if self.interval_type == "daily" else 'End Date'

    def _get_active_header(self):
        """Gets column header for days active, conditional on interval type."""
        if self.interval_type == "daily":
            return 'Was Active'
        elif self.interval_type == "weekly":
            return "Days Active This Week"
        else:
            return 'Days Active'

    def get_column_names(self):
        """
        List names of columns as they should appear in the CSV.

        Apart from the first two entries, these must also be the order
        they are stored in the Hive TSV output.
        """
        return [
            'Course ID',
            self._get_date_header(),
            'Username',
            'Email',
            'Cohort',
            self._get_active_header(),
            'Unique Problems Attempted',
            'Total Problem Attempts',
            'Unique Problems Correct',
            'Unique Videos Played',
            'Discussion Posts',
            'Discussion Responses',
            'Discussion Comments',
            'Discussion Upvotes Given',
            'Discussion Downvotes Given',
            'Discussion Upvotes Received',
            'Discussion Downvotes Received',
            'Textbook Pages Viewed',
            'URL of Last Subsection Viewed',
        ]

    def multi_output_reducer(self, key, values, output_file):
        """
        Each entry should be written to the output file in csv format.

        This output is visible to instructors, so use an excel friendly format (csv).
        """
        end_date, course_id = key
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
            row[self._get_date_header()] = end_date
            row['Course ID'] = course_id
            row_data.append(row)

        row_data = sorted(row_data, key=itemgetter(*field_names))

        for row_dict in row_data:
            # TSV's are assumed to be written (by Hive) in UTF-8 encoding,
            # so we should not encode the values of row_data before outputting.
            writer.writerow(row_dict)


class StudentEngagementTableTask(BareHiveTableTask):  # pragma: no cover
    """Creates the Hive storage table used to hold student_engagement_{interval_type} table data."""

    # Define interval_type here, instead of defining many parameters with a downstream mixin.
    interval_type = luigi.Parameter(default="daily")

    @property
    def partition_by(self):
        return 'dt'

    @property
    def table(self):
        return 'student_engagement_{interval_type}'.format(
            interval_type=self.interval_type
        )

    @property
    def columns(self):
        return StudentEngagementIntervalTypeRecord.get_hive_schema()


class StudentEngagementPartitionTask(StudentEngagementTableDownstreamMixin, HivePartitionTask):
    """Creates storage partition for the student_engagement_raw_{interval_type} Hive table."""

    @property
    def hive_table_task(self):
        return StudentEngagementTableTask(
            warehouse_path=self.warehouse_path,
            interval_type=self.interval_type,
        )

    @property
    def partition_value(self):
        return self.interval.date_b.isoformat()


class StudentEngagementDataTask(StudentEngagementTableDownstreamMixin, HiveQueryTask):
    """
    Execute the query on student_engagement_raw_{interval_type} Hive Table and persist the results
    into student_engagement_{interval_type} Hive table.
    """

    @property
    def insert_query(self):
        return """
        SELECT
            end_date,
            course_id,
            username,
            days_active,
            problems_attempted,
            problem_attempts,
            problems_correct,
            videos_played,
            forum_posts,
            forum_responses,
            forum_comments,
            forum_upvotes_given,
            forum_downvotes_given,
            forum_upvotes_received,
            forum_downvotes_received,
            textbook_pages_viewed,
            last_subsection_viewed
        FROM  student_engagement_raw_{interval_type}
        WHERE end_date >= '{start_date}' AND end_date < '{end_date}'
        """.format(
            interval_type=self.interval_type,
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat()
        )

    def query(self):  # pragma: no cover
        full_insert_query = """
        USE {database_name};

        INSERT INTO TABLE {table}
        PARTITION ({partition.query_spec})
        {insert_query}
        """.format(
            database_name=hive_database_name(),
            table=self.partition_task.hive_table_task.table,
            partition=self.partition,
            insert_query=self.insert_query.strip(),
        )
        return textwrap.dedent(full_insert_query)

    @property
    def partition(self):
        """A shorthand for the partition information on the upstream partition task."""
        return self.partition_task.partition

    @property
    def partition_task(self):
        """The task that creates partition on `student_engagement_{interval_type}` which is used for this job."""
        return StudentEngagementPartitionTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval_type=self.interval_type
        )

    def requires(self):
        for requirement in super(StudentEngagementDataTask, self).requires():
            yield requirement

        yield self.partition_task

        yield (
            StudentEngagementRawTableTask(
                mapreduce_engine=self.mapreduce_engine,
                n_reduce_tasks=self.n_reduce_tasks,
                source=self.source,
                interval=self.interval,
                pattern=self.pattern,
                interval_type=self.interval_type,
            )
        )

    def output(self):
        output_root = url_path_join(
            self.warehouse_path,
            self.partition_task.hive_table_task.table,
            self.partition.path_spec + '/'
        )
        return get_target_from_url(output_root, marker=True)

    def on_success(self):
        """Override the success method to touch the _SUCCESS file."""
        self.output().touch_marker()


class StudentEngagementToMysqlTask(StudentEngagementTableDownstreamMixin, MysqlInsertTask):
    """
    Copy the per-student engagement data from Hive to a MySQL table.
    """

    def __init__(self, *args, **kwargs):
        super(StudentEngagementToMysqlTask, self).__init__(*args, **kwargs)

        self.overwrite = True

    @property
    def table(self):
        return 'student_engagement_{}'.format(self.interval_type)

    @property
    def columns(self):
        return StudentEngagementIntervalTypeRecord.get_sql_schema()

    @property
    def auto_primary_key(self):
        # Instead of using an auto incrementing primary key, we define a custom compound primary key. See keys() defined
        # below. This vastly improves the performance of our most common query pattern.
        return None

    @property
    def keys(self):
        """
        Combine three fields that must be unique together as the primary key for this table.
        This dramatically speeds up access times at the cost of write speed.
        """
        # max length for this key must be under 3072 bytes; see comment in module_engagement.py
        return [
            ('PRIMARY KEY', ['course_id', 'username', 'end_date'])
        ]

    @property
    def insert_source_task(self):  # pragma: no cover
        return StudentEngagementDataTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            warehouse_path=self.warehouse_path,
            interval_type=self.interval_type
        )
