# -*- coding: utf-8 -*-
"""
Trajectory: Categorizes students into specific 'types' for each section of each course

There are nine types:
* Don't watch any videos, don't do any problems
* Don't watch any videos, do some problems
* Don't watch any videos, do all the problems
* Watch some videos, don't do any problems
* Watch some videos, do some problems
* Watch some videos, do all the problems
* Watch all the videos, don't do any problems
* Watch all the videos, do some problems
* Watch all the videos, do all the the problems


The final data report is broken down by course and by section, not by date.

All events are computed on arbitrary time intervals (e.g. weekly) for efficiency (it would be
preferable to always re-compute the data over all time, since the resulting information is not
date-specific, but that would not be feasible). However, for any chapter-user pair that has had
activity during that interval, the user's "type" of behavior in that chapter is re-computed
over all time. That way, if a user is marked as "watched some videos" in Chapter 1 during one
run of the pipeline, then later watches the rest of the videos, we will later re-evaluate that
user's activity over all time and mark them as "watched all videos" (when the pipeline is next
run).

Since there is currently no way for analytics tasks to know the structure of each course
(especially in cases where it varies from student to student), we make a number of assumptions:
   * That every student will have access to the same # of videos in each section
   * That every student will have access to the same # of problems in each section
     (if you are using content splits per cohort or randomized content, this could be false)
   * That for every week where some students are active, there is at least one student who
     starts/attempts all of the videos and problems that can be seen that week.
   * That every video/problem module ID occurs in only one place in the course (i.e. no two
     course sections contain the exact same video module as a descendant)


The pipeline tasks that compute the trajectory are as follows:

UserVideoViewingTask
        ▼
        ▼
PerUserVideoViewTask           ChapterAssociationTask           ProblemAttemptsPerUserTask
        ▼                                ▼                                 ▼
        ▼                                ▼                                 ▼
PerUserVideoViewTableTask      ChapterAssociationTableTask      ProblemAttemptsPerUserTableTask
        ▼                        ▼                   ▼                     ▼
        ▼                        ▼                   ▼                     ▼
    JoinedStudentChapterVideoActivityTask        JoinedStudentChapterProblemActivityTask
                                 ▼                   ▼
                                 ▼                   ▼
                               MergeTrajectorySourceDataTask
                                         ▼
                                         ▼
                               ComputeTrajectoryMaxPerChapterTask
                                         ▼
                                         ▼
                               MergeTrajectorySourceToMySQL
                                         ▼
                                         ▼
                               TrajectoryCountsTask
                                         ▼
                                         ▼
                               TrajectoryPipelineTask
"""
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import (
    WarehouseMixin,
    HivePartition,
    HiveTableTask,
    HiveTableFromQueryTask,
    HiveQueryToMysqlTask,
)
import logging
import luigi
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import UsageKey
import re
from .video import PerUserVideoViewTableTask, VIDEO_PLAYED

log = logging.getLogger(__name__)

PROBLEM_CHECK = 'problem_check'


class ChapterAssociationTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    For every course active in the specified interval, make a list of all the unique video and
    problem modules seen, recording the chapter ID of each one.

    This relies on http referer data sent by clients, so it's not guaranteed to be accurate. An
    alternative would be to pull the cached CourseStructure data from the LMS MySQL DB, but that
    does not have historical data.

    We assume:
    * That each course is a tree, not a DAG - i.e. that no module has multiple parents.
    * That no module is ever moved from one chapter to another.
    """
    COURSEWARE_URL_PATTERN = r'.*/courses/(?P<course_id>[^/]+)/courseware/(?P<chapter_id>[^/]+)/(?P<seq_id>[^/]+)/.*$'

    output_root = luigi.Parameter()

    def mapper(self, line):
        """ Filter events and separate them by block ID """
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        event_type = event.get('event_type')
        if event_type not in (VIDEO_PLAYED, PROBLEM_CHECK):
            return  # We don't care about this type of event

        referer_info = re.match(self.COURSEWARE_URL_PATTERN, event.get('referer'))
        if not referer_info:
            return
        chapter_id = referer_info.group('chapter_id')

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        block_type = None
        block_id = None
        if event_type == PROBLEM_CHECK:
            if event.get('event_source') != 'server':
                return

            try:
                usage_key = UsageKey.from_string(event_data.get('problem_id', ''))
            except InvalidKeyError:
                return
            block_type = usage_key.block_type
            block_id = usage_key.block_id
        elif event_type == VIDEO_PLAYED:
            block_id = event_data.get('id')
            block_type = 'video'

        if not block_type or not block_id:
            return

        yield ((course_id, block_type, block_id), (chapter_id))

    def reducer(self, key, chapter_ids):
        """ Save only the chapter ID associated with each block. """
        course_id, block_type, block_id = key
        chapter_ids = set(chapter_ids)

        if len(chapter_ids) > 1:
            log.error('Found multiple chapter IDs for %s with ID %s in course %s', block_type, block_id, course_id)

        chapter_id = chapter_ids.pop()

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            course_id.encode('utf-8'),
            block_type.encode('utf-8'),
            block_id.encode('utf-8'),
            chapter_id.encode('utf-8'),
        )

    def output(self):
        return get_target_from_url(self.output_root)


class ProblemAttemptsPerUserTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    In the given time interval, for each active student, make a list of the
    unique problem IDs attempted.
    """
    output_root = luigi.Parameter()

    def mapper(self, line):
        """ Filter events and separate them by username and course """
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, _date_string = value

        if event.get('event_type') != PROBLEM_CHECK or event.get('event_source') != 'server':
            return  # We don't care about this type of event

        course_id = eventlog.get_course_id(event)
        if not course_id:
            return

        event_data = eventlog.get_event_data(event)
        if event_data is None:
            return

        username = event.get('username', '').strip()
        if not username:
            return

        try:
            usage_key = UsageKey.from_string(event_data.get('problem_id', ''))
        except InvalidKeyError:
            return
        block_type = usage_key.block_type
        block_id = usage_key.block_id

        if not block_type or not block_id:
            return

        yield ((course_id, username), (block_type, block_id))

    def reducer(self, key, block_keys):
        """ Save only the unique blocks associated with each block. """
        course_id, username = key

        blocks_included = set()

        for block_key in block_keys:
            if block_key in blocks_included:
                continue
            blocks_included.add(block_key)
            block_type, block_id = block_key

            yield (
                # Output to be read by Hive must be encoded as UTF-8.
                course_id.encode('utf-8'),
                username.encode('utf-8'),
                block_type.encode('utf-8'),
                block_id.encode('utf-8'),
            )

    def output(self):
        return get_target_from_url(self.output_root)


class TrajectoryDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the tasks below."""
    @property
    def partition(self):
        """ Hive Partition used to store the new data being generated """
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member


class ChapterAssociationTableTask(TrajectoryDownstreamMixin, HiveTableTask):
    """ Hive table that stores the mapping from block IDs to chapter IDs """

    @property
    def table(self):
        return 'chapter_association'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('block_type', 'STRING'),
            ('block_id', 'STRING'),
            ('chapter_id', 'STRING'),
        ]

    def requires(self):
        return ChapterAssociationTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class ProblemAttemptsPerUserTableTask(TrajectoryDownstreamMixin, HiveTableTask):
    """ Hive table that stores the output of ProblemAttemptsPerUserTask """

    @property
    def table(self):
        return 'user_problem_attempts'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('username', 'STRING'),
            ('block_type', 'STRING'),
            ('block_id', 'STRING'),
        ]

    def requires(self):
        return ProblemAttemptsPerUserTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
        )


class JoinedStudentChapterVideoActivityTask(TrajectoryDownstreamMixin, HiveTableFromQueryTask):
    """
    Join the chapter-module association table data with per-student video data
    """

    @property
    def table(self):
        return 'per_student_videos_per_chapter'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('chapter_id', 'STRING'),
            ('username', 'STRING'),
            ('merge_key', 'STRING'),
            ('video_id', 'STRING'),
        ]

    @property
    def insert_query(self):
        return """
        SELECT
            ca.course_id,
            ca.chapter_id,
            vids.username,
            CONCAT(ca.course_id, '|', ca.chapter_id, '|', vids.username),
            vids.encoded_module_id
        FROM chapter_association ca
        INNER JOIN video_usage_per_user vids
            ON (ca.course_id = vids.course_id AND ca.block_id = vids.encoded_module_id)
        WHERE ca.block_type = 'video' AND ca.dt >= '{start_date}' AND ca.dt <= '{end_date}'
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
        )

    def requires(self):
        kwargs = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite': self.overwrite,
        }
        yield (
            ChapterAssociationTableTask(**kwargs),
            PerUserVideoViewTableTask(**kwargs),
        )


class JoinedStudentChapterProblemActivityTask(TrajectoryDownstreamMixin, HiveTableFromQueryTask):
    """
    Join the chapter-module association table data with per-student problem attempts data
    """

    @property
    def table(self):
        return 'per_student_problems_per_chapter'

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('chapter_id', 'STRING'),
            ('username', 'STRING'),
            ('merge_key', 'STRING'),
            ('problem_id', 'STRING'),
        ]

    @property
    def insert_query(self):
        return """
        SELECT
            ca.course_id,
            ca.chapter_id,
            probs.username,
            CONCAT(ca.course_id, '|', ca.chapter_id, '|', probs.username),
            CONCAT(probs.block_type, '|', probs.block_id)
        FROM chapter_association ca
        INNER JOIN user_problem_attempts probs
            ON (
                ca.course_id = probs.course_id AND
                ca.block_type = probs.block_type AND
                ca.block_id = probs.block_id
            )
        WHERE ca.dt >= '{start_date}' AND ca.dt <= '{end_date}'
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
        )

    def requires(self):
        kwargs = {
            'mapreduce_engine': self.mapreduce_engine,
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'overwrite': self.overwrite,
        }
        yield (
            ChapterAssociationTableTask(**kwargs),
            ProblemAttemptsPerUserTableTask(**kwargs),
        )


class MergeTrajectorySourceDataTask(TrajectoryDownstreamMixin, HiveTableFromQueryTask):
    """
    Take the per-user, per-chapter data from the two Joined tasks,
    count the unique videos and problems from each user, and output to a new
    table.

    The source data for this query in Hive was created in date
    partitions, but those date parittion boundaries will inevitably fall into
    the middle of some students' progress through certain chapters. So we query
    for a list of all the chapter IDs that changed during our current time
    interval, then update the data related to those chapters regardless of
    source partiition. If we'd already processed those chapters, we will process
    them again and overwrite the results.

    For example, if this task is run weekly and in week 1 a student does half
    of the problems in Chapter A, then the student will be marked as having done
    "some" problems. If in week 2 the student does the rest of the problems,
    we will re-calculate that chapter's data for that student and update the
    MySQL data to mark the student as having done "all" problems.
    """
    @property
    def table(self):
        return "trajectory_source_data"

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('chapter_id', 'STRING'),
            ('username', 'STRING'),
            ('num_videos', 'INT'),
            ('num_problems', 'INT'),
        ]

    @property
    def insert_query(self):
        # The query here is complex but is equivalent to the following:
        # 1. Select the distinct (course_id, chapter_id, username) (i.e. the "merge_key") values
        #    that changed during the current time interval.
        # 2. For each (course_id, chapter_id, username) tuple, select the number of videos
        #    watched (num_videos) and problems attempted (num_problems) over all time
        return """
        SELECT
            COALESCE(vids.course_id, probs.course_id),
            COALESCE(vids.chapter_id, probs.chapter_id),
            COALESCE(vids.username, probs.username),
            COALESCE(num_videos, 0),
            COALESCE(num_problems, 0)
        FROM (
            SELECT
                course_id,
                chapter_id,
                username,
                merge_key,
                COUNT(DISTINCT video_id) as num_videos
            FROM per_student_videos_per_chapter vids_middle
            WHERE merge_key IN (
                SELECT DISTINCT merge_key FROM per_student_videos_per_chapter
                WHERE dt >= '{start_date}' AND dt <= '{end_date}'
            )
            GROUP BY merge_key, course_id, chapter_id, username
        ) vids
        FULL OUTER JOIN (
            SELECT
                course_id,
                chapter_id,
                username,
                merge_key,
                COUNT(DISTINCT problem_id) as num_problems
            FROM per_student_problems_per_chapter probs_middle
            WHERE merge_key IN (
                SELECT DISTINCT merge_key FROM per_student_problems_per_chapter
                WHERE dt >= '{start_date}' AND dt <= '{end_date}'
            )
            GROUP BY merge_key, course_id, chapter_id, username
        ) probs
        ON vids.merge_key = probs.merge_key
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
        )

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            JoinedStudentChapterVideoActivityTask(**kwargs),
            JoinedStudentChapterProblemActivityTask(**kwargs),
        )


class ComputeTrajectoryMaxPerChapterTask(TrajectoryDownstreamMixin, HiveTableFromQueryTask):
    """
    Take the updated per-student per-chapter data (num_videos and num_problems),
    and compute the maximum # of videos watched and problems attempted by any
    student for each chapter.

    The analysis is only done within the current date-based Hive partition.
    """
    @property
    def table(self):
        return "trajectory_max_data"

    @property
    def columns(self):
        return [
            ('course_id', 'STRING'),
            ('chapter_id', 'STRING'),
            ('max_videos', 'INT'),
            ('max_problems', 'INT'),
        ]

    @property
    def insert_query(self):
        return """
        SELECT
            course_id,
            chapter_id,
            MAX(num_videos),
            MAX(num_problems)
        FROM trajectory_source_data
        WHERE dt >= '{start_date}' AND dt <= '{end_date}'
        GROUP BY course_id, chapter_id
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
        )

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            MergeTrajectorySourceDataTask(**kwargs),
        )


class MergeTrajectorySourceToMySQL(TrajectoryDownstreamMixin, HiveQueryToMysqlTask):
    """
    Combine the per-user trajectory data with the max-per-chapter data to
    generate the per-user per-chapter trajectory table data.
    """
    table = "trajectory_detail"
    # Because we process the incoming data in date-partitioned chunks, sometimes the new data
    # requires that we update a user's standing in a given chapter, bumping them from having
    # done "Some" problems/videos to "All".
    # We let MySQL handle this for us, by adding a constraint to the table and specifying
    # "REPLACE" instead of "INSERT"
    insert_query_template = "REPLACE INTO {table} ({column_names}) VALUES {values}"
    overwrite = luigi.BooleanParameter(default=False)

    @property
    def hive_columns(self):
        return [
            ('course_id', 'STRING'),
            ('chapter_id', 'STRING'),
            ('username', 'STRING'),
            ('video_type', 'INT'),
            ('problem_type', 'INT'),
        ]

    @property
    def columns(self):
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('chapter_id', 'VARCHAR(255) NOT NULL'),
            ('username', 'VARCHAR(255) NOT NULL'),
            ('video_type', 'TINYINT'),
            ('problem_type', 'TINYINT'),
        ]

    @property
    def default_columns(self):
        """ Columns and constraints that are managed by MySQL """
        return [
            ('created', 'TIMESTAMP DEFAULT NOW()'),
            ('CONSTRAINT ccu', 'UNIQUE (course_id, chapter_id, username)'),
        ]

    @property
    def query(self):
        return """
        SELECT
            ts.course_id,
            ts.chapter_id,
            ts.username,
            CASE
                WHEN num_videos = 0 THEN {none_value}
                WHEN num_videos = max_videos THEN {all_value}
                ELSE {some_value}
            END,
            CASE
                WHEN num_problems = 0 THEN {none_value}
                WHEN num_problems = max_problems THEN {all_value}
                ELSE {some_value}
            END
        FROM trajectory_source_data ts
        INNER JOIN trajectory_max_data mx ON ts.course_id = mx.course_id AND ts.chapter_id = mx.chapter_id
        WHERE ts.dt >= '{start_date}' AND ts.dt <= '{end_date}'
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
            all_value=2, some_value=1, none_value=0,
        )

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
            ComputeTrajectoryMaxPerChapterTask(**kwargs),
        )


class TrajectoryCountsTask(TrajectoryDownstreamMixin, HiveQueryToMysqlTask):
    """
    For every chapter with activity during the specified time interval, update
    the summary data in the 'trajectory' MySQL table using the data in the
    'trajectory_detail' MySQL table (we also use some data from Hive to limit
    the update to chapters that were active during the given interval).

    This step essentially eliminates the per-user data and stores only aggregate
    user counts.
    """
    overwrite = luigi.BooleanParameter(default=False)
    table = "trajectory"
    insert_query_template = """
        REPLACE INTO {table} (course_id, chapter_id, video_type, problem_type, num_users)
        SELECT course_id, chapter_id, video_type, problem_type, COUNT(username)
        FROM trajectory_detail WHERE ({column_names}) IN ({values})
        GROUP BY course_id, chapter_id, video_type, problem_type
    """

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
            MergeTrajectorySourceToMySQL(**kwargs),
        )

    @property
    def hive_columns(self):
        # All we need from Hive is the set of chapter IDs that changed during the current interval
        return [
            ('course_id', 'STRING'),
            ('chapter_id', 'STRING'),
        ]

    @property
    def columns(self):
        """ Columns whose data comes from Hive """
        return [
            ('course_id', 'VARCHAR(255) NOT NULL'),
            ('chapter_id', 'VARCHAR(255) NOT NULL'),
        ]

    @property
    def default_columns(self):
        """ Columns whose data comes from MySQL, plus constraints """
        return [
            ('video_type', 'TINYINT NOT NULL'),
            ('problem_type', 'TINYINT NOT NULL'),
            ('num_users', 'INTEGER NOT NULL'),
            ('created', 'TIMESTAMP DEFAULT NOW()'),
            ('CONSTRAINT ccu', 'UNIQUE (course_id, chapter_id, video_type, problem_type)'),
        ]

    @property
    def query(self):
        return """
        SELECT DISTINCT ts.course_id, ts.chapter_id
        FROM trajectory_source_data ts
        WHERE ts.dt >= '{start_date}' AND ts.dt <= '{end_date}'
        """.format(
            start_date=self.interval.date_a.isoformat(),
            end_date=self.interval.date_b.isoformat(),
        )


class TrajectoryPipelineTask(TrajectoryDownstreamMixin, luigi.WrapperTask):
    """ Run all the trajectory tasks and output the reports to MySQL. """

    def requires(self):
        kwargs = {
            'n_reduce_tasks': self.n_reduce_tasks,
            'source': self.source,
            'interval': self.interval,
            'pattern': self.pattern,
            'warehouse_path': self.warehouse_path,
        }
        yield (
            TrajectoryCountsTask(**kwargs),
        )
