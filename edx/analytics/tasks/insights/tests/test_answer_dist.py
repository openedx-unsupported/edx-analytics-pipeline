"""
Tests for tasks that calculate answer distributions.

"""
import hashlib
import json
import math
import os
import shutil
import StringIO
import tempfile
from unittest import TestCase

from mock import Mock, call
from opaque_keys.edx.locator import CourseLocator

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.insights.answer_dist import (
    AnswerDistributionOneFilePerCourseTask, AnswerDistributionPerCourseMixin, ProblemCheckEventMixin, try_str_to_float
)
from edx.analytics.tasks.util.tests.config import OPTION_REMOVED, with_luigi_config
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeLegacyKeysMixin, InitializeOpaqueKeysMixin


class ProblemCheckEventBaseTest(MapperTestMixin, ReducerTestMixin, TestCase):
    """Base test class for testing ProblemCheckEventMixin."""

    def initialize_ids(self):
        """Define set of id values for use in tests."""
        raise NotImplementedError

    def setUp(self):
        self.task_class = ProblemCheckEventMixin
        super(ProblemCheckEventBaseTest, self).setUp()

        self.initialize_ids()
        self.username = 'test_user'
        self.user_id = 24
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.earlier_timestamp = "2013-12-15T15:38:32.805444"
        self.reduce_key = (self.course_id, self.problem_id, self.username)  # pylint: disable=no-member

    def _create_event_data_dict(self, **kwargs):
        """Returns event data dict with test values."""
        event_data = {
            "problem_id": self.problem_id,
            "attempts": 2,
            "answers": {self.answer_id: "3"},
            "correct_map": {
                self.answer_id: {
                    "queuestate": None,
                    "npoints": None,
                    "msg": "",
                    "correctness": "incorrect",
                    "hintmode": None,
                    "hint": ""
                },
            },
            "state": {
                "input_state": {self.answer_id: None},
                "correct_map": None,
                "done": False,
                "seed": 1,
                "student_answers": {self.answer_id: "1"},
            },
            "grade": 0,
            "max_grade": 1,
            "success": "incorrect",
        }
        self._update_with_kwargs(event_data, **kwargs)
        return event_data

    @staticmethod
    def _update_with_kwargs(data_dict, **kwargs):
        """Updates a dict from kwargs only if it modifies a top-level value."""
        for key, value in kwargs.iteritems():
            if key in data_dict:
                data_dict[key] = value

    def _create_event_context(self, **kwargs):
        """Returns context dict with test values."""
        context = {
            "course_id": self.course_id,
            "org_id": self.org_id,
            "user_id": self.user_id,
        }
        self._update_with_kwargs(context, **kwargs)
        return context

    def _create_problem_data_dict(self, **kwargs):
        """Returns problem_data with test values."""
        problem_data = self._create_event_data_dict(**kwargs)
        problem_data['timestamp'] = self.timestamp
        problem_data['username'] = self.username
        problem_data['context'] = self._create_event_context(**kwargs)

        self._update_with_kwargs(problem_data, **kwargs)
        return problem_data

    def create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = {
            "username": self.username,
            "host": "test_host",
            "event_source": "server",
            "event_type": "problem_check",
            "context": self._create_event_context(**kwargs),
            "time": "{0}+00:00".format(self.timestamp),
            "ip": "127.0.0.1",
            "event": self._create_event_data_dict(**kwargs),
            "agent": "blah, blah, blah",
            "page": None
        }

        self._update_with_kwargs(event_dict, **kwargs)
        return event_dict


class ProblemCheckEventMapTest(InitializeOpaqueKeysMixin, ProblemCheckEventBaseTest):
    """Tests to verify that event log parsing by mapper works correctly."""

    def test_non_problem_check_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_unparseable_problem_check_event(self):
        line = 'this is garbage but contains problem_check'
        self.assert_no_map_output_for(line)

    def test_browser_event_source(self):
        line = self.create_event_log_line(event_source='browser')
        self.assert_no_map_output_for(line)

    def test_missing_event_source(self):
        line = self.create_event_log_line(event_source=None)
        self.assert_no_map_output_for(line)

    def test_missing_username(self):
        line = self.create_event_log_line(username=None)
        self.assert_no_map_output_for(line)

    def test_missing_event_type(self):
        # Here, we make the event as a dictionary so we can edit individual attributes of the event.
        event_dict = self.create_event_dict()
        event_dict['old_event_type'] = event_dict['event_type']
        del event_dict['event_type']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_implicit_problem_check_event_type(self):
        line = self.create_event_log_line(event_type='implicit/event/ending/with/problem_check')
        self.assert_no_map_output_for(line)

    def test_bad_datetime(self):
        line = self.create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_bad_event_data(self):
        line = self.create_event_log_line(event=["not an event"])
        self.assert_no_map_output_for(line)

    def test_missing_course_id(self):
        line = self.create_event_log_line(context={})
        self.assert_no_map_output_for(line)

    def test_illegal_course_id(self):
        line = self.create_event_log_line(course_id=";;;;bad/id/val")
        self.assert_no_map_output_for(line)

    def test_missing_problem_id(self):
        line = self.create_event_log_line(problem_id=None)
        self.assert_no_map_output_for(line)

    def test_missing_context(self):
        line = self.create_event_log_line(context=None)
        self.assert_no_map_output_for(line)

    def test_invalid_answer_id(self):
        self.answer_id = 'foo\nbar'
        line = self.create_event_log_line()
        self.assert_no_map_output_for(line)

    def test_invalid_answer_id_with_tab(self):
        self.answer_id = 'foo\tbar'
        line = self.create_event_log_line()
        self.assert_no_map_output_for(line)

    def test_invalid_answer_id_with_nonascii(self):
        self.answer_id = u"R\ufffd\ufffd\ufffdn\u0010.Y\u0001\ufffd.\ufffd\u0007\u0013*\ufffd\ufffd\ufffd\ufffdI\ufffd3}"
        line = self.create_event_log_line()
        self.assert_no_map_output_for(line)

    def test_good_problem_check_event(self):
        # Here, we make the event as a dictionary since we're comparing based on dictionaries anyway.
        event = self.create_event_dict()
        line = json.dumps(event)
        expected_data = self._create_problem_data_dict()
        # Apparently the output of json.dumps() is not consistent enough
        # to compare, due to ordering issues.  So compare the dicts
        # rather than the JSON strings.
        self.assert_single_map_output_load_jsons(line, self.reduce_key, (self.timestamp, expected_data))


class ProblemCheckEventLegacyMapTest(InitializeLegacyKeysMixin, ProblemCheckEventMapTest):
    """Run same mapper() tests, but using legacy values for keys."""
    pass


class ProblemCheckEventReduceTest(InitializeOpaqueKeysMixin, ProblemCheckEventBaseTest):
    """
    Verify that ProblemCheckEventMixin.reduce() works correctly.
    """

    def _check_output(self, inputs, expected):
        """
        Compare generated with expected output; we define an explicit check_output to meet the needs of answer
        distribution checking.

        Args:
            inputs: array of values to pass to reducer
                for hard-coded key.
            expected:  dict of expected answer data, with
                answer_id as key.

        """
        reducer_output = self._get_reducer_output(inputs)
        self.assertEquals(len(reducer_output), len(expected))
        for key, value in reducer_output:
            course_id, answer_id = key
            timestamp, answer_data = value
            answer_data = json.loads(answer_data)
            expected_key = "{}_{}".format(answer_id, answer_data["attempt_category"])
            self.assertEquals(course_id, self.course_id)
            self.assertEquals(timestamp, self.timestamp)
            self.assertTrue(expected_key in expected)
            self.assertEquals(answer_data, expected.get(expected_key))

    def _add_second_answer(self, problem_data, answer_id=None):
        """Adds a second answer to an existing problem check event."""
        if answer_id is None:
            answer_id = self.second_answer_id
        problem_data['answers'][answer_id] = "4"
        problem_data['correct_map'][answer_id] = {
            "correctness": "incorrect",
            "queuestate": None,
            "npoints": None,
            "msg": "",
            "hintmode": None,
            "hint": "",
        }

    def _get_answer_data(self, **kwargs):
        """Returns expected answer data returned by the reducer."""
        answer_data = {
            "answer_value_id": "3",
            "problem_display_name": None,
            "variant": 1,
            "correct": False,
            "problem_id": self.problem_id,
            "attempt_category": 'last',
            "answer_correct_map": {
                "queuestate": None,
                "npoints": None,
                "msg": "",
                "correctness": "incorrect",
                "hintmode": None,
                "hint": "",
            },
            "grade": 0,
            "max_grade": 1,
        }
        answer_data.update(**kwargs)
        return answer_data

    def _create_submission_problem_data_dict(self, **kwargs):
        """Returns problem event data with test values for 'submission'."""
        problem_data = self._create_problem_data_dict(**kwargs)
        problem_data_submission = {
            self.answer_id: {
                "input_type": "formulaequationinput",
                "question": "Enter the number of fingers on a human hand",
                "response_type": "numericalresponse",
                "answer": "3",
                "variant": "",
                "correct": False,
                "answer_correct_map": {
                    "queuestate": None,
                    "npoints": None,
                    "msg": "",
                    "correctness": "incorrect",
                    "hintmode": None,
                    "hint": ""
                },
                "grade": 0,
                "max_grade": 1,
            },
        }
        self._update_with_kwargs(problem_data_submission, **kwargs)
        if 'answer_value_id' in kwargs:
            problem_data_submission[self.answer_id]['answer_value_id'] = kwargs['answer_value_id']
        problem_data['submission'] = problem_data_submission
        return problem_data

    def _get_answer_data_from_submissions(self, problem_data, **kwargs):
        """Returns expected answer data returned by the reducer, given the event's data."""

        answer_data = {}
        first_submission = problem_data[0]['submission']
        last_submission = problem_data[-1]['submission']

        def insert_answer_data(submission, attempt_category):
            """
            Inserts each response included in submission into the expected answer data dictionary.

            Args:
                submission: dictionary of all responses submitted at once for a user
                attempt_category: a string that is 'first' for a user's first submission and 'last' otherwise
            """
            for answer_id, submission_data in submission.iteritems():
                answer_id_data = {
                    "answer": submission_data['answer'],
                    "problem_display_name": None,
                    "variant": submission_data['variant'],
                    "correct": submission_data['correct'],
                    "input_type": submission_data['input_type'],
                    "response_type": submission_data['response_type'],
                    "question": submission_data['question'],
                    "problem_id": self.problem_id,
                    "attempt_category": attempt_category,
                    "answer_correct_map": submission_data['answer_correct_map'],
                    "grade": submission_data['grade'],
                    "max_grade": submission_data['max_grade'],
                }
                if 'answer_value_id' in submission_data:
                    answer_id_data['answer_value_id'] = submission_data['answer_value_id']

                self._update_with_kwargs(answer_id_data, **kwargs)
                answer_data[self._get_submission_data_key(answer_id, attempt_category)] = answer_id_data

        insert_answer_data(first_submission, attempt_category='first')
        insert_answer_data(last_submission, attempt_category='last')

        return answer_data

    def _augment_single_submission_data(self, answer_data):
        """ Replicate response of ProblemCheckEventMixin.reduce on single submission. """
        first_response = answer_data.copy()
        last_response = answer_data.copy()

        first_response['attempt_category'] = 'first'
        last_response['attempt_category'] = 'last'

        return first_response, last_response

    def _get_submission_data_key(self, answer_id, attempt_category):
        """ Construct key to distinguish identical first and last responses. """
        return "{id}_{category}".format(id=answer_id, category=attempt_category)

    def test_no_events(self):
        self.assert_no_output([])

    def test_one_answer_event(self):
        problem_data = self._create_problem_data_dict()
        input_data = (self.timestamp, json.dumps(problem_data))
        answer_data = self._get_answer_data()
        first, last = self._augment_single_submission_data(answer_data)

        self._check_output(
            [input_data],
            {
                self._get_submission_data_key(self.answer_id, 'first'): first,
                self._get_submission_data_key(self.answer_id, 'last'): last,
            }
        )

    def test_one_correct_answer_event(self):
        answer_correct_map = {"correctness": "correct"}
        problem_data = self._create_problem_data_dict(
            correct_map={self.answer_id: answer_correct_map},
        )
        input_data = (self.timestamp, json.dumps(problem_data))
        answer_data = self._get_answer_data(correct=True, answer_correct_map=answer_correct_map)
        first, last = self._augment_single_submission_data(answer_data)

        self._check_output(
            [input_data],
            {
                self._get_submission_data_key(self.answer_id, 'first'): first,
                self._get_submission_data_key(self.answer_id, 'last'): last,
            }
        )

    def test_one_submission_event(self):
        problem_data = self._create_submission_problem_data_dict()
        input_data = (self.timestamp, json.dumps(problem_data))
        answer_data = self._get_answer_data_from_submissions([problem_data])
        self._check_output([input_data], answer_data)

    def test_one_submission_with_value_id(self):
        problem_data = self._create_submission_problem_data_dict(answer=3, answer_value_id='choice_3')
        input_data = (self.timestamp, json.dumps(problem_data))
        answer_data = self._get_answer_data_from_submissions([problem_data])
        self._check_output([input_data], answer_data)

    def test_one_submission_with_variant(self):
        problem_data = self._create_submission_problem_data_dict(variant=629)
        input_data = (self.timestamp, json.dumps(problem_data))
        answer_data = self._get_answer_data_from_submissions([problem_data])
        self._check_output([input_data], answer_data)

    def test_two_answer_event(self):
        problem_data = self._create_problem_data_dict()
        self._add_second_answer(problem_data)
        input_data = (self.timestamp, json.dumps(problem_data))

        answer_data = self._get_answer_data()
        first_answer, last_answer = self._augment_single_submission_data(answer_data)

        answer_data_2 = self._get_answer_data(answer_value_id="4")
        first_answer_2, last_answer_2 = self._augment_single_submission_data(answer_data_2)

        self._check_output([input_data], {
            self._get_submission_data_key(self.answer_id, 'first'): first_answer,
            self._get_submission_data_key(self.second_answer_id, 'first'): first_answer_2,
            self._get_submission_data_key(self.answer_id, 'last'): last_answer,
            self._get_submission_data_key(self.second_answer_id, 'last'): last_answer_2,
        })

    def test_two_answer_submission_event(self):
        problem_data = self._create_submission_problem_data_dict()
        problem_data_2 = self._create_submission_problem_data_dict(
            answer='4',
            variant=629,
            question="Enter the number of fingers on the other hand"
        )
        for key in ['answers', 'correct_map', 'submission']:
            problem_data[key][self.second_answer_id] = problem_data_2[key][self.answer_id]

        input_data = (self.timestamp, json.dumps(problem_data))
        answer_data = self._get_answer_data_from_submissions([problem_data])
        self._check_output([input_data], answer_data)

    def test_two_submission_event(self):
        problem_data = self._create_submission_problem_data_dict()
        problem_data_2 = self._create_submission_problem_data_dict(
            answer='7',
            time=self.earlier_timestamp,
        )

        input_data = [
            (self.timestamp, json.dumps(problem_data)),
            (self.earlier_timestamp, json.dumps(problem_data_2)),
        ]

        answer_data = self._get_answer_data_from_submissions([problem_data])
        self._check_output(input_data, answer_data)

    def test_hidden_answer_event(self):
        for hidden_suffix in ['_dynamath', '_comment']:
            problem_data = self._create_problem_data_dict()
            hidden_answer_id = "{answer_id}{suffix}".format(
                answer_id=self.answer_id, suffix=hidden_suffix
            )
            self._add_second_answer(problem_data, answer_id=hidden_answer_id)
            input_data = (self.timestamp, json.dumps(problem_data))
            answer_data = self._get_answer_data()
            first, last = self._augment_single_submission_data(answer_data)
            self._check_output([input_data], {
                self._get_submission_data_key(self.answer_id, 'first'): first,
                self._get_submission_data_key(self.answer_id, 'last'): last,
            })

    def test_bogus_choice_event(self):
        # In real data, values appeared in student_answers that were
        # not in the correct_map.  This was causing a failure.
        problem_data = self._create_problem_data_dict()
        del problem_data['answers'][self.answer_id]
        for bogus_value in ['choice_1', 'choice_2', 'choice_3']:
            bogus_answer_id = "{answer_id}_{suffix}".format(
                answer_id=self.answer_id, suffix=bogus_value
            )
            problem_data['answers'][bogus_answer_id] = bogus_value
            input_data = (self.timestamp, json.dumps(problem_data))
            # The problem should be skipped.
            self._check_output([input_data], {})

    def test_problem_display_name(self):
        problem_data = self._create_problem_data_dict()
        problem_data['context']['module'] = {'display_name': u"Displ\u0101y Name"}
        input_data = (self.timestamp, json.dumps(problem_data))
        answer_data = self._get_answer_data(problem_display_name=u"Displ\u0101y Name")

        first, last = self._augment_single_submission_data(answer_data)

        self._check_output(
            [input_data],
            {
                self._get_submission_data_key(self.answer_id, 'first'): first,
                self._get_submission_data_key(self.answer_id, 'last'): last,
            }
        )


class ProblemCheckEventLegacyReduceTest(ProblemCheckEventReduceTest, InitializeLegacyKeysMixin):
    """Run same reducer() tests, but using legacy values for keys."""
    pass


class AnswerDistributionPerCourseReduceTest(InitializeOpaqueKeysMixin, TestCase, ReducerTestMixin):
    """
    Verify that AnswerDistributionPerCourseMixin.reduce() works correctly.
    """
    def setUp(self):
        super(AnswerDistributionPerCourseReduceTest, self).setUp()
        self.initialize_ids()
        self.task = AnswerDistributionPerCourseMixin()
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.earlier_timestamp = "2013-12-15T15:38:32.805444"
        self.reduce_key = (self.course_id, self.answer_id)
        self.problem_display_name = "This is the Problem for You!"

    def _check_output(self, inputs, expected):
        """Compare generated with expected output."""
        reducer_output = self._get_reducer_output(inputs)
        self.assertEquals(len(reducer_output), len(expected))
        for course_id, _output in reducer_output:
            self.assertEquals(course_id, self.course_id)
        # We don't know what order the outputs will be dumped for a given
        # set of input dicts, so we have to compare sets of items.
        reducer_outputs = set([frozenset(json.loads(output).items()) for _, output in reducer_output])
        expected_outputs = set([frozenset(output.items()) for output in expected])
        self.assertEquals(reducer_outputs, expected_outputs)

    def _get_answer_data(self, **kwargs):
        """Returns answer data with submission information for input to reducer."""
        answer_data = {
            "answer": u"\u00b2",
            "problem_display_name": None,
            "variant": "",
            "correct": False,
            "problem_id": self.problem_id,
            "input_type": "formulaequationinput",
            "question": u"Enter the number(\u00ba) of fingers on a human hand",
            "response_type": "numericalresponse",
            "attempt_category": 'last',
        }
        answer_data.update(**kwargs)
        return answer_data

    def _get_non_submission_answer_data(self, **kwargs):
        """Returns answer data without submission information for input to reducer ."""
        answer_data = {
            "answer_value_id": u'\u00b2',
            "problem_display_name": None,
            "variant": "1",
            "correct": False,
            "problem_id": self.problem_id,
            "attempt_category": 'last',
        }
        answer_data.update(**kwargs)
        return answer_data

    def _get_expected_output(self, answer_data, **kwargs):
        """Get an expected reducer output based on the input."""
        expected_output = {
            "Problem Display Name": answer_data.get('problem_display_name') or "",
            "First Response Count": int(answer_data.get('attempt_category', 'first') == 'first'),
            "Last Response Count": int(answer_data.get('attempt_category', 'first') != 'first'),
            "PartID": self.answer_id,
            "Question": answer_data.get('question') or "",
            "AnswerValue": answer_data.get('answer') or answer_data.get('answer_value_id') or "",
            "ValueID": "",
            "Variant": answer_data.get('variant') or "",
            "Correct Answer": "1" if answer_data['correct'] else '0',
            "ModuleID": self.problem_id,
        }
        expected_output.update(**kwargs)
        return expected_output

    def test_no_user_counts(self):
        self.assert_no_output([])

    def test_one_answer_event(self):
        answer_data = self._get_answer_data()
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data)
        self._check_output([input_data], (expected_output,))

    def test_event_with_variant(self):
        answer_data = self._get_answer_data(variant=629)
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data)
        self._check_output([input_data], (expected_output,))

    def test_event_with_problem_name(self):
        answer_data = self._get_answer_data(problem_display_name=self.problem_display_name)
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data)
        self._check_output([input_data], (expected_output,))

    def check_choice_answer(self, answer, expected):
        """Run a choice answer with a provided value, and compare with expected."""
        answer_data = self._get_answer_data(
            answer_value_id='choice_1',
            answer=answer,
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data, ValueID='choice_1', AnswerValue=expected)
        self._check_output([input_data], (expected_output,))

    def test_choice_answer(self):
        self.check_choice_answer('First Choice', 'First Choice')

    def test_choice_answer_with_whitespace(self):
        self.check_choice_answer('First Choice\t', 'First Choice')

    def test_choice_answer_with_empty_string(self):
        self.check_choice_answer('', '')

    def test_choice_answer_with_empty_markup(self):
        self.check_choice_answer('<text><span>First Choice</span></text>', 'First Choice')

    def test_choice_answer_with_non_element_markup(self):
        # This tests a branch of the get_text_from_element logic,
        # where there is no tag on an element.
        self.check_choice_answer(
            '<text><span>First<!-- embedded comment --> Choice</span></text>',
            'First Choice'
        )

    def test_choice_answer_with_html_markup(self):
        self.check_choice_answer('<p>First<br>Choice', 'First Choice')

    def test_choice_answer_with_embedded_whitespace(self):
        self.check_choice_answer('First  \t\n    Choice  ', 'First Choice')

    def test_choice_answer_with_bad_html_markup(self):
        self.check_choice_answer('<p First <br>Choice', 'Choice')

    def test_choice_answer_with_bad2_html_markup(self):
        self.check_choice_answer('First br>Choice', 'First br>Choice')

    def test_choice_answer_with_cdata_html_markup(self):
        self.check_choice_answer('First <![CDATA[This is to be ignored.]]>  Choice', 'First Choice')

    def test_multiple_choice_answer(self):
        answer_data = self._get_answer_data(
            answer_value_id=['choice_1', 'choice_2', 'choice_4'],
            answer=[u'First Ch\u014dice', u'Second Ch\u014dice', u'Fourth Ch\u014dice'],
            response_type="multiplechoiceresponse",
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(
            answer_data,
            ValueID='[choice_1|choice_2|choice_4]',
            AnswerValue=u'[First Ch\u014dice|Second Ch\u014dice|Fourth Ch\u014dice]'
        )
        self._check_output([input_data], (expected_output,))

    def test_multiple_choice_answer_with_markup(self):
        answer_data = self._get_answer_data(
            answer_value_id=['choice_1', 'choice_2', 'choice_4'],
            answer=[
                u'<text>First Ch\u014dice</text>',
                u'Second <sup>Ch\u014dice</sup>',
                u'Fourth <table><tbody><tr><td>Ch\u014dice</td></tr></tbody></table> goes here.'
            ],
            response_type="multiplechoiceresponse",
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(
            answer_data,
            ValueID='[choice_1|choice_2|choice_4]',
            AnswerValue=u'[First Ch\u014dice|Second Ch\u014dice|Fourth Ch\u014dice goes here.]'
        )
        self._check_output([input_data], (expected_output,))

    def test_filtered_response_type(self):
        answer_data = self._get_answer_data(
            response_type="nonsenseresponse",
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        self.assert_no_output([input_data])

    @with_luigi_config('answer-distribution', 'valid_response_types', OPTION_REMOVED)
    def test_filtered_response_type_default(self):
        answer_data = self._get_answer_data(
            response_type="nonsenseresponse",
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        self.assert_no_output([input_data])

    @with_luigi_config('answer-distribution', 'valid_response_types', OPTION_REMOVED)
    def test_valid_response_type_default(self):
        answer_data = self._get_answer_data(
            response_type="multiplechoiceresponse",
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data)
        self._check_output([input_data], (expected_output,))

    @with_luigi_config('answer-distribution', 'valid_response_types', 'multiplechoiceresponse,numericalresponse')
    def test_filtered_response_type_with_config(self):
        answer_data = self._get_answer_data(
            response_type="nonsenseresponse",
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        self.assertEquals(self._get_reducer_output([input_data]), tuple())

    @with_luigi_config('answer-distribution', 'valid_response_types', 'multiplechoiceresponse,numericalresponse')
    def test_valid_response_type_with_config(self):
        answer_data = self._get_answer_data(
            response_type="multiplechoiceresponse",
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data)
        self._check_output([input_data], (expected_output,))

    def test_filtered_non_submission_answer(self):
        answer_data = self._get_non_submission_answer_data()
        input_data = (self.timestamp, json.dumps(answer_data))
        self.assert_no_output([input_data])

    def test_two_answer_event_same(self):
        answer_data = self._get_answer_data()
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data))
        input_data_2 = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data, **{'Last Response Count': 2})
        self._check_output([input_data_1, input_data_2], (expected_output,))

    def test_two_answer_event_same_reversed(self):
        answer_data = self._get_answer_data()
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data))
        input_data_2 = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(answer_data, **{'Last Response Count': 2})
        self._check_output([input_data_2, input_data_1], (expected_output,))

    def test_two_answer_event_same_old_and_new(self):
        answer_data_1 = self._get_non_submission_answer_data()
        answer_data_2 = self._get_answer_data()
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output = self._get_expected_output(answer_data_2, **{'Last Response Count': 2})
        self._check_output([input_data_1, input_data_2], (expected_output,))

    def test_same_old_and_new_with_variant(self):
        answer_data_1 = self._get_non_submission_answer_data(variant=123)
        answer_data_2 = self._get_answer_data(variant=123)
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output = self._get_expected_output(answer_data_2, **{'Last Response Count': 2})
        self._check_output([input_data_1, input_data_2], (expected_output,))

    def test_two_answer_event_different_answer(self):
        answer_data_1 = self._get_answer_data(answer="first")
        answer_data_2 = self._get_answer_data(answer="second")
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output_1 = self._get_expected_output(answer_data_1)
        expected_output_2 = self._get_expected_output(answer_data_2)
        self._check_output([input_data_1, input_data_2], (expected_output_1, expected_output_2))

    def test_two_answer_event_different_answer_by_whitespace(self):
        answer_data_1 = self._get_answer_data(answer="\t\n\nfirst   ")
        answer_data_2 = self._get_answer_data(answer="first")
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output = self._get_expected_output(answer_data_2, **{'Last Response Count': 2})
        self._check_output([input_data_1, input_data_2], (expected_output,))

    def test_two_answer_event_different_old_and_new(self):
        answer_data_1 = self._get_non_submission_answer_data(answer_value_id="first")
        answer_data_2 = self._get_answer_data(problem_display_name=self.problem_display_name)
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output_2 = self._get_expected_output(answer_data_2)
        # An older non-submission-based event should not inherit
        # information from a newer submission-based event.
        expected_output_1 = self._get_expected_output(answer_data_1)
        expected_output_1['Problem Display Name'] = expected_output_2['Problem Display Name']
        self._check_output([input_data_1, input_data_2], (expected_output_1, expected_output_2))

    def test_two_answer_event_different_variant(self):
        answer_data_1 = self._get_answer_data(variant=123)
        answer_data_2 = self._get_answer_data(variant=456)
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output_1 = self._get_expected_output(answer_data_1)
        expected_output_2 = self._get_expected_output(answer_data_2)
        self._check_output([input_data_1, input_data_2], (expected_output_1, expected_output_2))

    def test_two_answer_event_different_variant_empty_new(self):
        answer_data_1 = self._get_answer_data(variant=123)
        answer_data_2 = self._get_answer_data(variant='')
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output_1 = self._get_expected_output(answer_data_1)
        expected_output_2 = self._get_expected_output(answer_data_2)
        self._check_output([input_data_1, input_data_2], (expected_output_1, expected_output_2))

    def test_problem_type_changed_to_multi_choice(self):
        answer_data_1 = self._get_answer_data(
            answer=u'First Ch\u014dice',
            response_type='optionresponse',
        )
        answer_data_2 = self._get_answer_data(
            answer_value_id=['choice_1', 'choice_2', 'choice_4'],
            answer=[u'First Ch\u014dice', u'Second Ch\u014dice', u'Fourth Ch\u014dice'],
            response_type="multiplechoiceresponse",
        )
        input_data_1 = (self.earlier_timestamp, json.dumps(answer_data_1))
        input_data_2 = (self.timestamp, json.dumps(answer_data_2))
        expected_output_1 = self._get_expected_output(answer_data_1)
        expected_output_2 = self._get_expected_output(
            answer_data_2,
            ValueID='[choice_1|choice_2|choice_4]',
            AnswerValue=u'[First Ch\u014dice|Second Ch\u014dice|Fourth Ch\u014dice]'
        )
        self._check_output([input_data_1, input_data_2], (expected_output_1, expected_output_2))

    def _load_metadata(self, **kwargs):
        """Defines some metadata for test answer."""
        metadata_dict = {
            self.answer_id: {
                "question": u"Pick One or \u00b2",
                "response_type": "multiplechoiceresponse",
                "input_type": "my_input_type",
                "problem_display_name": self.problem_display_name,
            }
        }
        metadata_dict[self.answer_id].update(**kwargs)
        answer_metadata = StringIO.StringIO(json.dumps(metadata_dict))
        self.task.load_answer_metadata(answer_metadata)

    def test_non_submission_choice_with_metadata(self):
        self._load_metadata(
            answer_value_id_map={"choice_1": u"First Ch\u014dice", "choice_2": u"Second Ch\u014dice"}
        )
        answer_data = self._get_non_submission_answer_data(
            answer_value_id='choice_1',
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(
            answer_data,
            ValueID='choice_1',
            AnswerValue=u'First Ch\u014dice',
            Question=u"Pick One or \u00b2",
        )
        expected_output["Problem Display Name"] = self.problem_display_name
        self._check_output([input_data], (expected_output,))

    def test_non_submission_multichoice_with_metadata(self):
        self._load_metadata(
            answer_value_id_map={"choice_1": "First Choice", "choice_2": "Second Choice"}
        )
        answer_data = self._get_non_submission_answer_data(
            answer_value_id=['choice_1', 'choice_2']
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(
            answer_data,
            ValueID='[choice_1|choice_2]',
            AnswerValue='[First Choice|Second Choice]',
            Question=u"Pick One or \u00b2",
        )
        expected_output["Problem Display Name"] = self.problem_display_name

        self._check_output([input_data], (expected_output,))

    def test_non_submission_nonmapped_multichoice_with_metadata(self):
        self._load_metadata()
        answer_data = self._get_non_submission_answer_data(
            answer_value_id=['choice_1', 'choice_2']
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(
            answer_data,
            ValueID='[choice_1|choice_2]',
            AnswerValue='',
            Question=u"Pick One or \u00b2",
        )
        expected_output["Problem Display Name"] = self.problem_display_name
        self._check_output([input_data], (expected_output,))

    def test_non_submission_nonmapped_choice_with_metadata(self):
        self._load_metadata()
        answer_data = self._get_non_submission_answer_data(
            answer_value_id='choice_1'
        )
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(
            answer_data,
            ValueID='choice_1',
            AnswerValue='',
            Question=u"Pick One or \u00b2",
        )
        expected_output["Problem Display Name"] = self.problem_display_name
        self._check_output([input_data], (expected_output,))

    def test_non_submission_nonmapped_nonchoice_with_metadata(self):
        self._load_metadata(response_type="optionresponse")
        answer_data = self._get_non_submission_answer_data()
        input_data = (self.timestamp, json.dumps(answer_data))
        expected_output = self._get_expected_output(
            answer_data,
            AnswerValue=u'\u00b2',
            Question=u"Pick One or \u00b2",
        )
        expected_output["Problem Display Name"] = self.problem_display_name
        self._check_output([input_data], (expected_output,))


class AnswerDistributionPerCourseLegacyReduceTest(InitializeLegacyKeysMixin, AnswerDistributionPerCourseReduceTest):
    """
    Verify that AnswerDistributionPerCourseMixin.reduce() works correctly
    with legacy ids.

    """
    pass


class AnswerDistributionOneFilePerCourseTaskTest(MapperTestMixin, ReducerTestMixin, TestCase):
    """Tests for AnswerDistributionOneFilePerCourseTask class."""

    def setUp(self):
        self.task_class = AnswerDistributionOneFilePerCourseTask
        super(AnswerDistributionOneFilePerCourseTaskTest, self).setUp()

        self.task = AnswerDistributionOneFilePerCourseTask(
            mapreduce_engine='local',
            src=[],
            dest=None,
            name=None,
            include=[],
            output_root=None,
        )

    def test_map_single_value(self):
        self.assert_single_map_output('foo\tbar', 'foo', 'bar')

    def test_reduce_multiple_values(self):
        field_names = AnswerDistributionPerCourseMixin.get_column_order()

        # To test sorting, the first sample is made to sort after the
        # second sample.
        column_values_2 = [(k, unicode(k) + u'\u2603') for k in field_names]
        column_values_2[3] = (column_values_2[3][0], 10)
        column_values_1 = list(column_values_2)
        column_values_1[4] = (column_values_1[4][0], u'ZZZZZZZZZZZ')
        sample_input_1 = json.dumps(dict(column_values_1))
        sample_input_2 = json.dumps(dict(column_values_2))
        mock_output_file = Mock()

        self.task.multi_output_reducer('foo', iter([sample_input_1, sample_input_2]), mock_output_file)

        expected_header_string = ','.join(field_names) + '\r\n'
        self.assertEquals(mock_output_file.write.mock_calls[0], call(expected_header_string))

        # Confirm that the second sample appears before the first.
        expected_row_1 = ','.join(unicode(v[1]).encode('utf8') for v in column_values_2) + '\r\n'
        self.assertEquals(mock_output_file.write.mock_calls[1], call(expected_row_1))
        expected_row_2 = ','.join(unicode(v[1]).encode('utf8') for v in column_values_1) + '\r\n'
        self.assertEquals(mock_output_file.write.mock_calls[2], call(expected_row_2))

    def test_output_path_for_legacy_key(self):
        course_id = 'foo/bar/baz'
        hashed_course_id = hashlib.sha1(course_id).hexdigest()
        task = AnswerDistributionOneFilePerCourseTask(
            mapreduce_engine='local',
            src=[],
            dest=None,
            name='name',
            include=[],
            output_root='/tmp',
        )
        output_path = task.output_path_for_key(course_id)
        expected_output_path = '/tmp/{0}/foo_bar_baz_answer_distribution.csv'.format(hashed_course_id)
        self.assertEquals(output_path, expected_output_path)

    def test_output_path_for_opaque_key(self):
        course_id = str(CourseLocator(org='foo', course='bar', run='baz'))
        hashed_course_id = hashlib.sha1(course_id).hexdigest()
        task = AnswerDistributionOneFilePerCourseTask(
            mapreduce_engine='local',
            src=[],
            dest=None,
            name='name',
            include=[],
            output_root='/tmp',
        )
        output_path = task.output_path_for_key(course_id)
        expected_output_path = '/tmp/{0}/foo_bar_baz_answer_distribution.csv'.format(hashed_course_id)
        self.assertEquals(output_path, expected_output_path)


class AnswerDistributionOneFilePerCourseTaskOutputRootTest(TestCase):
    """Tests for output_root behavior of AnswerDistributionOneFilePerCourseTask."""

    def setUp(self):
        # Define a real output directory, so it can
        # be removed if existing.
        def cleanup(dirname):
            """Remove the temp directory only if it exists."""
            if os.path.exists(dirname):
                shutil.rmtree(dirname)

        self.output_root = tempfile.mkdtemp()
        self.addCleanup(cleanup, self.output_root)

    def test_no_delete_output_root(self):
        # Not using the delete_output_root option will
        # not delete the output_root.
        self.assertTrue(os.path.exists(self.output_root))
        AnswerDistributionOneFilePerCourseTask(
            mapreduce_engine='local',
            src=[],
            dest=None,
            name='name',
            include=[],
            output_root=self.output_root,
        )
        self.assertTrue(os.path.exists(self.output_root))

    def test_delete_output_root(self):
        # It's still possible to use the delete option
        # to get rid of the output_root directory.
        task = AnswerDistributionOneFilePerCourseTask(
            mapreduce_engine='local',
            src=[],
            dest=None,
            name='name',
            include=[],
            output_root=self.output_root,
            delete_output_root="true",
            marker=self.output_root,
        )
        self.assertFalse(task.complete())
        self.assertFalse(os.path.exists(self.output_root))


class TestHelperFunctions(TestCase):
    """
    Test cases for helper functions
    """
    EXPECTED_RESULTS = (
        (u"234", 234, 234),
        (u"-1.5", -1.5, -1.5),
        (35.8, 35.8, 35.8)
    )

    EXPECTED_CONVERSION_EXCEPTIONS = (
        (u"-3d", ValueError, None),
        (Exception, TypeError, None),
        (str, TypeError, None),
    )

    NAN_STRINGS = (u"NaN", u"nan")

    INFINITY_STRINGS = (u"inf", u"-inf", u"infinity", u"-infinity")

    def test_try_str_to_float_no_exception(self):
        """
        Tests try_str_to_float for cases where float() doesn't throw an exception
        """
        for testval, float_output, expected in self.EXPECTED_RESULTS:
            self.assertEqual(float_output, float(testval))
            self.assertEqual(expected, try_str_to_float(testval))

    def test_try_str_to_float_with_exception(self):
        """
        Tests try_str_to_float for cases where float() throws an exception
        """
        for testval, float_exception, expected in self.EXPECTED_CONVERSION_EXCEPTIONS:
            with self.assertRaises(float_exception):
                float(testval)
            self.assertEqual(expected, try_str_to_float(testval))

    def test_try_str_to_float_nan(self):
        """
        Tests try_str_to_float for NaN-like values.  We want it to return None
        """
        for testval in self.NAN_STRINGS:
            self.assertTrue(math.isnan(float(testval)))
            self.assertIsNone(try_str_to_float(testval))

    def test_try_str_to_float_infinity(self):
        """
        Tests try_str_to_float for infinity-like values.  We want it to return None
        """
        for testval in self.INFINITY_STRINGS:
            self.assertTrue(math.isinf(float(testval)))
            self.assertIsNone(try_str_to_float(testval))
