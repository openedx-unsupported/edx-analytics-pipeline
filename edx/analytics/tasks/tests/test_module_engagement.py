"""Test metrics for student engagement with modules"""

import json
import datetime

import luigi
from ddt import ddt, data, unpack
from mock import MagicMock

from edx.analytics.tasks.module_engagement import ModuleEngagementDataTask, ModuleEngagementSummaryDataTask, \
    ModuleEngagementRecord, ModuleEngagementSummaryRecord, ModuleEngagementSummaryMetricRangesDataTask, \
    ModuleEngagementSummaryMetricRangeRecord, ModuleEngagementUserSegmentDataTask, ModuleEngagementUserSegmentRecord
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin, InitializeLegacyKeysMixin
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.target import FakeTarget


@ddt
class ModuleEngagementTaskMapTest(InitializeOpaqueKeysMixin, MapperTestMixin, unittest.TestCase):
    """Base class for test analysis of detailed student engagement"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    def setUp(self):
        super(ModuleEngagementTaskMapTest, self).setUp()

        self.initialize_ids()
        self.video_id = 'i4x-foo-bar-baz'
        self.forum_id = 'a2cb123f9c2146f3211cdc6901acb00e'
        self.event_templates = {
            'play_video': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "browser",
                "event_type": "play_video",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": '{"id": "%s", "currentTime": "23.4398", "code": "87389iouhdfh"}' % self.video_id,
                "agent": "blah, blah, blah",
                "page": None
            },
            'problem_check': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "problem_check",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "ip": "127.0.0.1",
                "event": {
                    "problem_id": self.problem_id,
                    "success": "incorrect",
                },
                "agent": "blah, blah, blah",
                "page": None
            },
            'edx.forum.object.created': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "edx.forum.comment.created",
                "name": "edx.forum.comment.created",
                "time": "{0}+00:00".format(self.DEFAULT_TIMESTAMP),
                "event": {
                    "commentable_id": self.forum_id,
                },
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.DEFAULT_USER_ID,
                },
                "ip": "127.0.0.1",
                "page": None,
            }
        }
        self.default_event_template = 'problem_check'
        self.create_task()

    def create_task(self, date=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not date:
            date = self.DEFAULT_DATE
        self.task = ModuleEngagementDataTask(
            date=luigi.DateParameter().parse(date),
            output_root='/fake/output',
        )
        self.task.init_local()

    @data(
        {'time': "2013-12-01T15:38:32.805444"},
        {'username': ''},
        {'event_type': None},
        {'context': {'course_id': 'lskdjfslkdj'}},
        {'event': 'sdfasdf'}
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    def test_browser_problem_check_event(self):
        template = self.event_templates['problem_check']
        self.assert_no_map_output_for(self.create_event_log_line(template=template, event_source='browser'))

    def test_incorrect_problem_check(self):
        self.assert_single_map_output(
            json.dumps(self.event_templates['problem_check']),
            self.get_expected_output_key('problem', self.problem_id, 'attempted'),
            1
        )

    def get_expected_output_key(self, entity_type, entity_id, action):
        """Generate the expected key"""
        return self.course_id, 'test_user', self.DEFAULT_DATE, entity_type, entity_id, action

    def test_correct_problem_check(self):
        template = self.event_templates['problem_check']
        template['event']['success'] = 'correct'

        self.assert_map_output(
            json.dumps(template),
            [
                (self.get_expected_output_key('problem', self.problem_id, 'completed'), 1),
                (self.get_expected_output_key('problem', self.problem_id, 'attempted'), 1)
            ]
        )

    def test_missing_problem_id(self):
        template = self.event_templates['problem_check']
        del template['event']['problem_id']
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    def test_missing_video_id(self):
        template = self.event_templates['play_video']
        template['event'] = '{"currentTime": "23.4398", "code": "87389iouhdfh"}'
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    def test_play_video(self):
        self.assert_single_map_output(
            json.dumps(self.event_templates['play_video']),
            self.get_expected_output_key('video', self.video_id, 'viewed'),
            1
        )

    def test_missing_forum_id(self):
        template = self.event_templates['edx.forum.object.created']
        del template['event']['commentable_id']
        self.assert_no_map_output_for(self.create_event_log_line(template=template))

    @data(
        ('edx.forum.comment.created', 'contributed'),
        ('edx.forum.response.created', 'contributed'),
        ('edx.forum.thread.created', 'contributed'),
    )
    @unpack
    def test_forum_posting_events(self, event_type, expected_action):
        template = self.event_templates['edx.forum.object.created']
        template['event_type'] = event_type
        template['name'] = event_type
        self.assert_single_map_output(
            json.dumps(template),
            self.get_expected_output_key('discussion', self.forum_id, expected_action),
            1
        )


class ModuleEngagementTaskMapLegacyKeysTest(InitializeLegacyKeysMixin, ModuleEngagementTaskMapTest):
    """Also test with legacy keys"""
    pass


@ddt
class ModuleEngagementTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """
    Tests to verify that engagement data is reduced properly
    """

    task_class = ModuleEngagementDataTask

    def setUp(self):
        super(ModuleEngagementTaskReducerTest, self).setUp()

        self.reduce_key = (self.COURSE_ID, 'test_user', self.DATE, 'problem', 'foobar', 'completed')

    def test_replacement_of_count(self):
        inputs = [1, 1, 1, 1]
        self._check_output_complete_tuple(
            inputs,
            (('\t'.join(self.reduce_key), 4),)
        )


@ddt
class ModuleEngagementSummaryDataTaskMapTest(MapperTestMixin, unittest.TestCase):
    """Base class for test analysis of student engagement summaries"""

    task_class = ModuleEngagementSummaryDataTask

    input_record = ModuleEngagementRecord(
        course_id='foo/bar/baz',
        username='foouser',
        date=datetime.date(2015, 11, 1),
        entity_type='problem',
        entity_id='problem-id',
        event='attempted',
        count=1
    )

    def test_invalid_input_types(self):
        input_as_strings = list(self.input_record.to_string_tuple())
        input_as_strings[-1] = 'foo'
        with self.assertRaises(ValueError):
            tuple(self.task.mapper('\t'.join(input_as_strings) + '\n'))

    def test_not_enough_input_columns(self):
        input_as_strings = list(self.input_record.to_string_tuple())[:-1]
        with self.assertRaises(ValueError):
            tuple(self.task.mapper('\t'.join(input_as_strings) + '\n'))

    def test_single_output(self):
        tsv_line = self.input_record.to_separated_values()
        self.assert_single_map_output(
            tsv_line,
            ('foo/bar/baz', 'foouser'),
            tsv_line
        )


@ddt
class ModuleEngagementSummaryDataTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """Base class for test analysis of student engagement summaries"""

    task_class = ModuleEngagementSummaryDataTask
    output_record_type = ModuleEngagementSummaryRecord

    input_record = ModuleEngagementRecord(
        course_id='foo/bar/baz',
        username='test_user',
        date=datetime.date(2014, 03, 26),
        entity_type='problem',
        entity_id='problem-id',
        event='attempted',
        count=1
    )

    def setUp(self):
        super(ModuleEngagementSummaryDataTaskReducerTest, self).setUp()

        self.reduce_key = (self.COURSE_ID, 'test_user')

        self.video_play_record = self.input_record.replace(
            entity_type='video',
            entity_id='foox-video1',
            event='viewed',
        )
        self.forum_record = self.input_record.replace(
            entity_type='discussion',
            entity_id='forum0',
            event='contributed'
        )

    def test_output_format(self):
        self._check_output_complete_tuple(
            [self.input_record.to_separated_values()],
            (
                (
                    'foo/bar/baz',
                    'test_user',
                    '2014-03-25',
                    '2014-04-01',
                    '1',
                    '1',
                    '0',
                    'inf',
                    '0',
                    '0',
                    '1',
                ),
            )
        )

    def test_multiple_problem_attempts_same_problem(self):
        self._check_output_by_record_field(
            [
                self.input_record.to_separated_values(),
                self.input_record.replace(date=datetime.date(2014, 03, 27)).to_separated_values()
            ],
            {
                'problem_attempts': '2',
                'problems_attempted': '1',
                'problem_attempts_per_completion': 'inf',
                'days_active': '2'
            }
        )

    def test_multiple_problem_attempts_different_problem(self):
        self._check_output_by_record_field(
            [
                self.input_record.to_separated_values(),
                self.input_record.replace(entity_id='problem-id-2').to_separated_values()
            ],
            {
                'problem_attempts': '2',
                'problems_attempted': '2',
                'problem_attempts_per_completion': 'inf',
                'days_active': '1'
            }
        )

    def test_correct_problem_attempt(self):
        self._check_output_by_record_field(
            [
                self.input_record.to_separated_values(),
                self.input_record.replace(event='completed').to_separated_values()
            ],
            {
                'problem_attempts': '1',
                'problems_attempted': '1',
                'problems_completed': '1',
                'problem_attempts_per_completion': str(1.0),
                'days_active': '1'
            }
        )

    def test_correct_problem_multiple_attempts(self):
        self._check_output_by_record_field(
            [
                self.input_record.replace(count=4).to_separated_values(),
                self.input_record.replace(event='completed').to_separated_values()
            ],
            {
                'problem_attempts': '4',
                'problems_attempted': '1',
                'problems_completed': '1',
                'problem_attempts_per_completion': str(4.0),
                'days_active': '1'
            }
        )

    def test_multiple_correct_problems(self):
        self._check_output_by_record_field(
            [
                self.input_record.replace(count=4).to_separated_values(),
                self.input_record.replace(event='completed').to_separated_values(),
                self.input_record.replace(date=datetime.date(2014, 03, 27), entity_id='p2').to_separated_values(),
                self.input_record.replace(
                    date=datetime.date(2014, 03, 27),
                    entity_id='p2',
                    event='completed',
                ).to_separated_values(),
            ],
            {
                'problem_attempts': '5',
                'problems_attempted': '2',
                'problems_completed': '2',
                'problem_attempts_per_completion': str(2.5),
                'days_active': '2'
            }
        )

    def test_video_viewed(self):
        self._check_output_by_record_field(
            [
                self.video_play_record.to_separated_values(),
            ],
            {
                'problem_attempts': '0',
                'problems_attempted': '0',
                'problems_completed': '0',
                'problem_attempts_per_completion': str(0.0),
                'videos_viewed': '1',
                'days_active': '1'
            }
        )

    def test_multiple_videos_viewed(self):
        self._check_output_by_record_field(
            [
                self.video_play_record.to_separated_values(),
                self.video_play_record.replace(entity_id='1').to_separated_values(),
            ],
            {
                'videos_viewed': '2'
            }
        )

    def test_multiple_forum_contributions(self):
        self._check_output_by_record_field(
            [
                self.forum_record.to_separated_values(),
                self.forum_record.replace(entity_id='1', count=2).to_separated_values(),
            ],
            {
                'discussions_contributed': '3'
            }
        )


@ddt
class ModuleEngagementSummaryMetricRangesDataTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """Base class for test analysis of student engagement summaries"""

    task_class = ModuleEngagementSummaryMetricRangesDataTask
    output_record_type = ModuleEngagementSummaryMetricRangeRecord

    def setUp(self):
        super(ModuleEngagementSummaryMetricRangesDataTaskReducerTest, self).setUp()

        self.reduce_key = 'foo/bar/baz'
        self.input_record = ModuleEngagementSummaryRecord(
            course_id='foo/bar/baz',
            username='test_user',
            start_date=datetime.date(2014, 3, 25),
            end_date=datetime.date(2014, 4, 1),
            problem_attempts=0,
            problems_attempted=0,
            problems_completed=0,
            problem_attempts_per_completion=0.0,
            videos_viewed=0,
            discussions_contributed=0,
            days_active=0,
        )

    def test_output_format(self):
        self._check_output_complete_tuple(
            [self.input_record.replace(videos_viewed=1).to_separated_values()],
            (
                (
                    'foo/bar/baz',
                    '2014-03-25',
                    '2014-04-01',
                    'videos_viewed',
                    'low',
                    '0.9',
                    '1.0',
                ),
                (
                    'foo/bar/baz',
                    '2014-03-25',
                    '2014-04-01',
                    'videos_viewed',
                    'high',
                    '1.0',
                    '1.1',
                ),
            )
        )

    def test_simple_distribution(self):
        # [0, 0, 0, 0] (these values are dropped from the set before analyzing)
        # [4, 13, 13, 13] (3 records are <= 13, this accounts for 15% of the total 20 non-zero values)
        # [15] * 4 (throw in a bunch of data in the "normal" range)
        # [50] * 11 (round out the 20 records with some other arbitrary value, note that this will also contain two
        #    of the three highest values)
        # [154] (throw in an outlier - a very high maximum value, this will show the high end of the range, but the
        #    85th percentile should be at the 50 value)
        values = [4] + ([13] * 3) + ([0] * 4) + ([15] * 4) + ([50] * 11) + [154]

        self.assert_ranges(
            values,
            3.9,
            13.0,
            50.0,
            154.1
        )

    def assert_ranges(self, values, minimum, low, high, maximum):
        """Given a list of values, assert that the ranges generated have the min, low, high, and max bounds."""

        # Manufacture some records with these values
        records = [self.input_record.replace(problem_attempts_per_completion=v).to_separated_values() for v in values]

        self._check_output_complete_tuple(
            records,
            (
                (
                    'foo/bar/baz',
                    '2014-03-25',
                    '2014-04-01',
                    'problem_attempts_per_completion',
                    'low',
                    str(minimum),
                    str(low),
                ),
                (
                    'foo/bar/baz',
                    '2014-03-25',
                    '2014-04-01',
                    'problem_attempts_per_completion',
                    'high',
                    str(high),
                    str(maximum),
                ),
            )
        )

    def test_identical_values(self):
        values = [5] * 6
        self.assert_ranges(values, 4.9, 5.0, 5.0, 5.1)

    def test_single_value(self):
        self.assert_ranges([1], 0.9, 1.0, 1.0, 1.1)

    def test_very_small_values(self):
        self.assert_ranges(([0.01] * 10) + ([0.09] * 10), -0.09, 0.01, 0.09, 0.19)

    def test_infinite_value(self):
        self.assert_ranges(([1.0] * 19) + [float('inf')], 0.9, 1.0, 1.0, float('inf'))

    def test_negative_infinite_value(self):
        self.assert_ranges(([1.0] * 19) + [float('-inf')], float('-inf'), 1.0, 1.0, 1.1)


@ddt
class ModuleEngagementUserSegmentDataTaskReducerTest(ReducerTestMixin, unittest.TestCase):
    """Base class for test analysis of student engagement summaries"""

    task_class = ModuleEngagementUserSegmentDataTask
    output_record_type = ModuleEngagementUserSegmentRecord

    def setUp(self):
        self.course_id = 'foo/bar/baz'
        self.username = 'test_user'
        self.prev_week_start_date = datetime.date(2014, 3, 18)
        self.start_date = datetime.date(2014, 3, 25)
        self.date = datetime.date(2014, 4, 1)

        self.reduce_key = (self.course_id, self.username)

        self.input_record = ModuleEngagementSummaryRecord(
            course_id=self.course_id,
            username=self.username,
            start_date=self.start_date,
            end_date=self.date,
            problem_attempts=0,
            problems_attempted=0,
            problems_completed=0,
            problem_attempts_per_completion=0.0,
            videos_viewed=0,
            discussions_contributed=0,
            days_active=0,
        )

        self.range_record = ModuleEngagementSummaryMetricRangeRecord(
            course_id=self.course_id,
            start_date=self.start_date,
            end_date=self.date,
            metric='problems_attempted',
            range_type='high',
            low_value=5.0,
            high_value=10.0
        )

        self.task = self.task_class(  # pylint: disable=not-callable
            date=self.date,
            output_root=self.DEFAULT_ARGS['output_root']
        )

    def initialize_task(self, metric_ranges):
        """Given a list of metric ranges, setup the task by calling init_local"""
        metric_ranges_text = '\n'.join([
            r.to_separated_values()
            for r in metric_ranges
        ])

        self.task.input_local = MagicMock(return_value=FakeTarget(value=metric_ranges_text))
        self.task.init_local()

    def test_init_local(self):
        other_course_record = self.range_record.replace(
            course_id='another/course/id',
            metric='problems_completed'
        )
        self.initialize_task([
            self.range_record,
            self.range_record.replace(
                range_type='low',
                low_value=0.0,
                high_value=3.0
            ),
            other_course_record
        ])

        self.assertEqual(dict(self.task.high_metric_ranges), {
            self.course_id: {
                'problems_attempted': self.range_record
            },
            'another/course/id': {
                'problems_completed': other_course_record
            }
        })

    def test_init_local_empty_input(self):
        self.initialize_task([])
        self.assertEqual(dict(self.task.high_metric_ranges), {})

    def test_output_format(self):
        self.initialize_task([
            self.range_record,
            self.range_record.replace(
                metric='problem_attempts_per_completion',
                low_value=8.0,
                high_value=10.1
            )
        ])
        self._check_output_complete_tuple(
            [
                self.input_record.replace(
                    problems_attempted=6,
                    problem_attempts_per_completion=9
                ).to_separated_values()
            ],
            (
                (
                    'foo/bar/baz',
                    'test_user',
                    '2014-03-25',
                    '2014-04-01',
                    'highly_engaged',
                    'problems_attempted'
                ),
                (
                    'foo/bar/baz',
                    'test_user',
                    '2014-03-25',
                    '2014-04-01',
                    'struggling',
                    'problem_attempts_per_completion'
                ),
            )
        )

    @data(
        'problems_attempted',
        'problems_completed',
        'videos_viewed',
        'discussions_contributed'
    )
    def test_highly_engaged(self, metric):
        self.initialize_task([
            self.range_record.replace(
                metric=metric
            )
        ])
        self._check_output_by_record_field(
            [
                self.input_record.replace(
                    **{metric: 8}
                ).to_separated_values()
            ],
            {
                'segment': 'highly_engaged'
            }
        )

    @data(
        'problem_attempts',
        'problem_attempts_per_completion',
    )
    def test_not_highly_engaged(self, metric):
        self.initialize_task([
            self.range_record.replace(
                metric=metric
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    **{metric: 8}
                ).to_separated_values()
            ]
        )
        self.assert_not_in_segment(output, 'highly_engaged')

    def assert_not_in_segment(self, output, segment):
        """Assert that the user was not put into the provided segment."""
        for record_tuple in output:
            record = self.output_record_type.from_string_tuple(record_tuple)
            self.assertNotEqual(record.segment, segment)

    def test_highly_engaged_too_low(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problems_completed'
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problems_completed=0
                ).to_separated_values()
            ]
        )
        self.assert_not_in_segment(output, 'highly_engaged')

    def test_highly_engaged_left_closed_interval_bottom(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problems_completed',
                low_value=6.0
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problems_completed=6
                ).to_separated_values()
            ]
        )
        self.assert_in_segment(output, 'highly_engaged')

    def assert_in_segment(self, output, segment):
        """Assert that the user was put into the provided segment."""
        for record_tuple in output:
            record = self.output_record_type.from_string_tuple(record_tuple)
            if record.segment == segment:
                return True
        return False

    def test_highly_engaged_left_closed_interval_top(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problems_completed',
                high_value=9.0
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problems_completed=9
                ).to_separated_values()
            ]
        )
        self.assert_not_in_segment(output, 'highly_engaged')

    def test_disengaging(self):
        self.initialize_task([])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    start_date=self.prev_week_start_date,
                    end_date=self.start_date,
                    days_active=1,
                ).to_separated_values()
            ]
        )
        self.assert_in_segment(output, 'disengaging')

    def test_not_disengaging_both_active(self):
        self.initialize_task([])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    start_date=self.prev_week_start_date,
                    end_date=self.start_date,
                    days_active=1,
                ).to_separated_values(),
                self.input_record.replace(
                    days_active=1
                ).to_separated_values()
            ]
        )
        self.assert_not_in_segment(output, 'disengaging')

    def test_not_disengaging_only_recent(self):
        self.initialize_task([])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    days_active=1
                ).to_separated_values()
            ]
        )
        self.assert_not_in_segment(output, 'disengaging')

    def test_struggling(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problem_attempts_per_completion',
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problem_attempts_per_completion=8.0
                ).to_separated_values()
            ]
        )
        self.assert_in_segment(output, 'struggling')

    def test_struggling_infinite_low_high_value(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problem_attempts_per_completion',
                low_value=float('inf'),
                high_value=float('inf'),
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problem_attempts_per_completion=float('inf')
                ).to_separated_values()
            ]
        )
        self.assert_in_segment(output, 'struggling')

    def test_struggling_infinite_high(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problem_attempts_per_completion',
                high_value=float('inf'),
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problem_attempts_per_completion=10.0
                ).to_separated_values()
            ]
        )
        self.assert_in_segment(output, 'struggling')

    def test_struggling_infinite_high_value(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problem_attempts_per_completion',
                high_value=float('inf'),
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problem_attempts_per_completion=float('inf')
                ).to_separated_values()
            ]
        )
        self.assert_in_segment(output, 'struggling')

    def test_not_struggling(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problem_attempts_per_completion',
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problem_attempts_per_completion=3.0
                ).to_separated_values()
            ]
        )
        self.assert_not_in_segment(output, 'struggling')

    def test_not_struggling_infinite_low(self):
        self.initialize_task([
            self.range_record.replace(
                metric='problem_attempts_per_completion',
                low_value=float('inf')
            )
        ])
        output = self._get_reducer_output(
            [
                self.input_record.replace(
                    problem_attempts_per_completion=100000.0
                ).to_separated_values()
            ]
        )
        self.assert_not_in_segment(output, 'struggling')
