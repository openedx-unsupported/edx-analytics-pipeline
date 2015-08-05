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

All events are computed on a weekly basis for efficiency (it would be preferable to always
re-compute the data over all time, since the resulting information is not date-specific, but
that would obviously not be feasible). Since there is currently no way for analytics tasks to
know the structure of each course (especially in cases where it varies from student to student),
we make a number of assumptions:
   * That every student will have access to the same # of videos in each section
   * That every student will have access to the same # of problems in each section
     (if you are using content splits per cohort or randomized content, this could be false)
   * That for every week where some students are active, there is at least one student who
     starts/attempts all of the videos and problems that can be seen that week.
   * That every video/problem module ID occurs in only one place in the course (i.e. no two
     course sections contain the exact same video module as a descendant)
"""
import datetime
from edx.analytics.tasks.mapreduce import MapReduceJobTask, MapReduceJobTaskMixin
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url
from edx.analytics.tasks.util import eventlog
from edx.analytics.tasks.util.hive import WarehouseMixin, HivePartition, HiveTableTask, HiveQueryToMysqlTask
import logging
import luigi
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import UsageKey
import re
from .video import VIDEO_PLAYED

log = logging.getLogger(__name__)

PROBLEM_CHECK = 'problem_check'


class ChapterAssociationTask(EventLogSelectionMixin, MapReduceJobTask):
    """
    For every course and every date interval, make a list of all the unique video and problem
    modules seen, recording the chapter ID of each one.

    This relies on http referer data sent by clients, so it's not guaranteed to be accurate. An
    alternative would be to pull the cached CourseStructure data from the LMS MySQL DB, but that
    does not have historical data.
    """
    COURSEWARE_URL_PATTERN = r'.*/courses/(?P<course_id>[^/]+)/courseware/(?P<chapter_id>[^/]+)/(?P<seq_id>[^/]+)/.*$'

    output_root = luigi.Parameter()
    interval_type = luigi.Parameter()

    def mapper(self, line):
        """ Filter events and separate them by block ID """
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

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
        else:
            raise NotImplementedError("Only weekly intervals are currently supported for ChapterAssociationTask")

        yield ((date_grouping_key, course_id, block_type, block_id), (chapter_id))

    def reducer(self, key, chapter_ids):
        """ Save only the chapter ID associated with each block. """
        date_grouping_key, course_id, block_type, block_id = key
        chapter_ids = set(chapter_ids)
        
        if len(chapter_ids) > 1:
            log.error('Found multiple chapter IDs for %s with ID %s in course %s', block_type, block_id, course_id)

        chapter_id = chapter_ids.pop()

        yield (
            # Output to be read by Hive must be encoded as UTF-8.
            date_grouping_key,
            course_id.encode('utf-8'),
            block_type.encode('utf-8'),
            block_id.encode('utf-8'),
            chapter_id.encode('utf-8'),
        )

    def output(self):
        return get_target_from_url(self.output_root)


class TrajectoryDownstreamMixin(WarehouseMixin, EventLogSelectionDownstreamMixin, MapReduceJobTaskMixin):
    """All parameters needed to run the tasks below."""
    interval_type = luigi.Parameter(default="weekly")


class ChapterAssociationTableTask(TrajectoryDownstreamMixin, HiveTableTask):
    """ Hive table that stores the mapping from block IDs to chapter IDs """

    @property
    def table(self):
        return 'chapter_association_{}'.format(self.interval_type)

    @property
    def columns(self):
        return [
            ('end_date', 'STRING'),
            ('course_id', 'STRING'),
            ('block_type', 'STRING'),
            ('block_id', 'STRING'),
            ('chapter_id', 'STRING'),
        ]

    @property
    def partition(self):
        return HivePartition('dt', self.interval.date_b.isoformat())  # pylint: disable=no-member

    def requires(self):
        return ChapterAssociationTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=self.partition_location,
            interval_type=self.interval_type,
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
            ChapterAssociationTableTask(**kwargs),
        )
