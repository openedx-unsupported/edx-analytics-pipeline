"""Test tags distribution"""
from unittest import TestCase

import luigi

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.insights.tags_dist import TagsDistributionPerCourse
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class TagsDistributionPerCourseMapTest(MapperTestMixin, InitializeOpaqueKeysMixin, TestCase):
    """
    Tests to verify that event log parsing by mapper works correctly.
    """
    def setUp(self):
        self.task_class = TagsDistributionPerCourse
        super(TagsDistributionPerCourseMapTest, self).setUp()

        self.initialize_ids()

        self.user_id = 42
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.problem_name = 'Test test test'
        self.saved_tags = {'difficulty': 'Hard', 'learning_outcome': 'Learned everything'}
        self.usage_key = u"block-v1:{course_id}+type@problem+block@ffb8df09604f4e73ac0".format(course_id=self.course_id)

        self.event_templates = {
            'tags_distribution_event': {
                "username": "test_user",
                "host": "test_host",
                "event_source": "server",
                "event_type": "problem_check",
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": self.user_id,
                    "module": {
                        "usage_key": self.usage_key,
                        "display_name": self.problem_name,
                    },
                    "asides": {
                        "tagging_aside": {
                            "saved_tags": self.saved_tags,
                        }
                    }
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "course_id": self.course_id,
                    "user_id": self.user_id,
                    "problem_id": self.problem_id,
                    "mode": "honor",
                    "success": "correct",
                }
            }
        }
        self.default_event_template = 'tags_distribution_event'

        self.expected_key = (self.course_id, self.org_id, self.problem_id)

    def test_non_problem_check_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_problem_check_event(self):
        line = 'this is garbage but contains problem_check'
        self.assert_no_map_output_for(line)

    def test_non_problem_check_event_type(self):
        line = self.create_event_log_line(event_type='edx.course.enrollment.unknown')
        self.assert_no_map_output_for(line)

    def test_bad_datetime(self):
        line = self.create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_bad_event_data(self):
        line = self.create_event_log_line(event=["not an event"])
        self.assert_no_map_output_for(line)

    def test_illegal_course_id(self):
        line = self.create_event_log_line(event={"course_id": ";;;;bad/id/val", "user_id": self.user_id})
        self.assert_no_map_output_for(line)

    def test_illegal_event_source(self):
        line = self.create_event_log_line(event_source='client')
        self.assert_no_map_output_for(line)

    def test_empty_problem_id(self):
        event = {
            "course_id": self.course_id,
            "user_id": self.user_id,
            "mode": "honor",
            "success": "incorrect",
        }
        line = self.create_event_log_line(event=event)
        self.assert_no_map_output_for(line)

    def test_good_problem_check_correct_answer_event(self):
        line = self.create_event_log_line()
        expected_value = (self.timestamp, self.saved_tags, 1)
        self.assert_single_map_output(line, self.expected_key, expected_value)

    def test_good_problem_check_incorrect_answer_event(self):
        event = {
            "course_id": self.course_id,
            "user_id": self.user_id,
            "problem_id": self.problem_id,
            "mode": "honor",
            "success": "incorrect",
        }
        line = self.create_event_log_line(event=event)
        expected_value = (self.timestamp, self.saved_tags, 0)
        self.assert_single_map_output(line, self.expected_key, expected_value)

    def test_good_problem_check_with_empty_tags_event(self):
        context = {
            "course_id": self.course_id,
            "org_id": self.org_id,
            "user_id": self.user_id,
            "module": {
                "usage_key": self.usage_key,
                "display_name": self.problem_name,
            }
        }

        line = self.create_event_log_line(context=context)
        expected_value = (self.timestamp, {}, 1)
        self.assert_single_map_output(line, self.expected_key, expected_value)


class TagsDistributionPerCourseReducerTest(ReducerTestMixin, TestCase):
    """
    Tests to verify that events-per-day-per-user reducer works correctly.
    """
    def setUp(self):
        self.task_class = TagsDistributionPerCourse
        super(TagsDistributionPerCourseReducerTest, self).setUp()

        # Create the task locally, since we only need to check certain attributes
        self.create_task_distribution_task()
        self.problem_id = 'problem_id'
        self.course_id = 'foo/bar/baz'
        self.org_id = 'foo'
        self.reduce_key = (self.course_id, self.org_id, self.problem_id)
        self.saved_tags = {'difficulty': 'Hard', 'learning_outcome': 'Learned everything'}

    def create_task_distribution_task(self, interval='2013-01-01'):
        """Create a task for testing purposes."""
        fake_param = luigi.DateIntervalParameter()
        self.task = TagsDistributionPerCourse(
            interval=fake_param.parse(interval),
            output_root="/fake/output",
        )

    def test_no_events(self):
        self.assert_no_output([])

    def test_single_simple_event(self):
        inputs = [('2013-01-01T00:00:01', self.saved_tags, 1), ]
        expected = ((self.course_id, self.org_id, self.problem_id, 'difficulty', 'Hard', '1', '1'),
                    (self.course_id, self.org_id, self.problem_id, 'learning_outcome', 'Learned everything', '1', '1'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_many_events(self):
        inputs = [('2013-01-01T00:00:0{sec}'.format(sec=k), self.saved_tags,
                   1 if k != 1 else 0) for k in xrange(4)]
        expected = ((self.course_id, self.org_id, self.problem_id, 'difficulty', 'Hard', '4', '3'),
                    (self.course_id, self.org_id, self.problem_id, 'learning_outcome', 'Learned everything', '4', '3'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_many_events_but_all_incorrect(self):
        inputs = [('2013-01-01T00:00:0{sec}'.format(sec=k), self.saved_tags, 0) for k in xrange(4)]
        expected = ((self.course_id, self.org_id, self.problem_id, 'difficulty', 'Hard', '4', '0'),
                    (self.course_id, self.org_id, self.problem_id, 'learning_outcome', 'Learned everything', '4', '0'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_the_last_tag_and_problem_name_should_be_taken(self):
        inputs = [('2013-01-01T00:00:03', {'difficulty': 'Hard',
                                           'learning_outcome': 'Learned nothing'}, 1),
                  ('2013-01-01T00:00:04', {'difficulty': 'Easy',
                                           'learning_outcome': 'Learned everything'}, 0),
                  ('2013-01-01T00:00:01', {'difficulty': 'Medium',
                                           'learning_outcome': 'Learned nothing'}, 1),
                  ('2013-01-01T00:00:02', {'difficulty': 'Hard',
                                           'learning_outcome': 'Learned nothing'}, 1), ]

        expected = ((self.course_id, self.org_id, self.problem_id, 'difficulty', 'Easy', '4', '3'),
                    (self.course_id, self.org_id, self.problem_id, 'learning_outcome', 'Learned everything', '4', '3'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_one_tag_was_removed_in_the_last_event(self):
        inputs = [('2013-01-01T00:00:03', {'difficulty': 'Hard',
                                           'learning_outcome': 'Learned nothing'}, 1),
                  ('2013-01-01T00:00:04', {'difficulty': 'Easy'}, 0),
                  ('2013-01-01T00:00:01', {'difficulty': 'Medium',
                                           'learning_outcome': 'Learned nothing'}, 1),
                  ('2013-01-01T00:00:02', {'difficulty': 'Hard',
                                           'learning_outcome': 'Learned nothing'}, 1), ]

        expected = ((self.course_id, self.org_id, self.problem_id, 'difficulty', 'Easy', '4', '3'),)
        self._check_output_complete_tuple(inputs, expected)

    def test_all_tags_were_removed_in_the_last_event(self):
        inputs = [('2013-01-01T00:00:03', {'difficulty': 'Hard',
                                           'learning_outcome': 'Learned nothing'}, 1),
                  ('2013-01-01T00:00:04', {}, 0),
                  ('2013-01-01T00:00:01', {'difficulty': 'Medium',
                                           'learning_outcome': 'Learned nothing'}, 1),
                  ('2013-01-01T00:00:02', {'difficulty': 'Hard',
                                           'learning_outcome': 'Learned nothing'}, 1), ]
        self._check_output_complete_tuple(inputs, ())

    def test_multiple_tag_values(self):
        multiple_tag_values = {
            'learning_outcome_1': ['Research as Inquiry', 'Authority is Constructed and Contextual'],
            'learning_outcome_2': ['Research as Inquiry', 'Scholarship as Conversation'],
        }
        inputs = [('2013-01-01T00:00:0{sec}'.format(sec=k), multiple_tag_values,
                   1 if k != 1 else 0) for k in xrange(4)]
        expected = ((self.course_id, self.org_id, self.problem_id, 'learning_outcome_1',
                     'Research as Inquiry', '4', '3'),
                    (self.course_id, self.org_id, self.problem_id, 'learning_outcome_1',
                     'Authority is Constructed and Contextual', '4', '3'),
                    (self.course_id, self.org_id, self.problem_id, 'learning_outcome_2',
                     'Research as Inquiry', '4', '3'),
                    (self.course_id, self.org_id, self.problem_id, 'learning_outcome_2',
                     'Scholarship as Conversation', '4', '3'))
        self._check_output_complete_tuple(inputs, expected)
