
from itertools import groupby
from operator import itemgetter
import re
import gzip

import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin, EventLogSelectionDownstreamMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join
from edx.analytics.tasks.util import eventlog


class StudentEngagementTask(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

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
        elif event_type[:9] == '/courses/' and re.match(r'/courses/[^/+]+(/|\+)[^/+]+(/|\+)[^/]+/courseware/[^/]+/[^/]+/$', event_type):
            info['path'] = event_type
            info['timestamp'] = timestamp
            event_type = 'marker:last_subsection_viewed'

        yield ((date_string, course_id, username), (entity_id, event_type, info))

    def reducer(self, key, events):
        date_string, course_id, username = key
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

        yield date_string, course_id, username, was_active, num_problems_attempted, num_problem_attempts, num_problems_correct, num_videos_played, num_forum_posts, num_forum_replies, num_forum_comments, num_textbook_pages, last_subsection_viewed

    def output(self):
        return get_target_from_url(self.output_root)


class SplitStudentEngagementTask(EventLogSelectionDownstreamMixin, MultiOutputMapReduceJobTask):

    output_root = luigi.Parameter()

    COLUMN_NAMES = (
        'course_id',
        'username',
        'was_active',
        'problems_attempted',
        'problem_attempts',
        'problems_correct',
        'videos_played',
        'forum_posts',
        'forum_replies',
        'forum_comments',
        'textbook_pages_viewed',
        'last_subsection_viewed'
    )

    def requires(self):
        yield StudentEngagementTask(
            mapreduce_engine=self.mapreduce_engine,
            n_reduce_tasks=self.n_reduce_tasks,
            source=self.source,
            interval=self.interval,
            pattern=self.pattern,
            output_root=url_path_join(self.output_root, 'raw')
        )

    def mapper(self, line):
        split_line = line.split('\t')
        yield (split_line[0], tuple(split_line[1:]))

    def multi_output_reducer(self, _date_string, values, output_file):
        with gzip.GzipFile(mode='wb', fileobj=output_file) as compressed_output_file:
            compressed_output_file.write('\t'.join(self.COLUMN_NAMES) + '\n')
            for value in values:
                compressed_output_file.write('\t'.join(value))
                compressed_output_file.write('\n')

    def output_path_for_key(self, date_string):
        return url_path_join(
            self.output_root,
            'output',
            date_string + '.tsv.gz'
        )
