"""Test problem response data tasks."""

import json
import os
import random
import re
import shutil
import tempfile
from collections import namedtuple
from datetime import datetime
from unittest import TestCase

import luigi
from ddt import data, ddt, unpack

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.insights.problem_response import (
    LatestProblemResponseDataTask, LatestProblemResponseTableTask, ProblemResponseRecord, ProblemResponseReportTask
)
from edx.analytics.tasks.util.record import DateTimeField
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeLegacyKeysMixin, InitializeOpaqueKeysMixin


class ProblemResponseTestMixin(InitializeOpaqueKeysMixin):
    """
    Helper methods for testing the problem response tasks.
    """
    task_class = LatestProblemResponseDataTask

    def setUp(self):
        self.setup_dirs()
        super(ProblemResponseTestMixin, self).setUp()

    def setup_dirs(self):
        """Create temp input and output dirs."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_rootdir, "output")
        os.mkdir(self.output_dir)
        self.input_dir = os.path.join(self.temp_rootdir, "input")
        os.mkdir(self.input_dir)
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)


@ddt
class LatestProblemResponseTaskMapTest(ProblemResponseTestMixin, MapperTestMixin, TestCase):
    """Tests the latest problem response mapper."""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    def setUp(self):
        super(LatestProblemResponseTaskMapTest, self).setUp()

        self.initialize_ids()
        self.event_templates = {
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
                },
                "agent": "blah, blah, blah",
                "page": None
            },
        }
        self.default_event_template = 'problem_check'
        self.create_task()

    def create_task(self, interval=None):  # pylint: disable=arguments-differ
        """Allow arguments to be passed to the task constructor."""
        if not interval:
            interval = self.DEFAULT_DATE
        self.task = LatestProblemResponseDataTask(
            interval=luigi.DateIntervalParameter().parse(interval),
            output_root='/fake/output',
        )
        self.task.init_local()

    @data(
        {'time': "2013-12-01T15:38:32.805444"},
        {'username': ''},
        {'event_type': None},
        {'event_source': None},
        {'context': {'course_id': 'lskdjfslkdj'}},
        {'event': 'sdfasdf'}
    )
    def test_invalid_events(self, kwargs):
        self.assert_no_map_output_for(self.create_event_log_line(**kwargs))

    def test_browser_problem_check_event(self):
        template = self.event_templates['problem_check']
        self.assert_no_map_output_for(self.create_event_log_line(template=template, event_source='browser'))

    def test_incorrect_problem_check(self):
        self.assert_single_map_output_load_jsons(
            json.dumps(self.event_templates['problem_check']),
            self.get_expected_output_key(),
            self.get_expected_output_value_dict(),
        )

    def get_expected_output_key(self, problem_id=None):
        """Generate the expected key tuple"""
        if not problem_id:
            problem_id = self.problem_id
        return self.course_id, problem_id, 'test_user'

    def get_expected_output_value_dict(self, problem_id=None):
        """Generate the expected value tuple.
           Returns a dict instead of the JSON value, to avoid ordering issues."""
        if not problem_id:
            problem_id = self.problem_id
        event = self.event_templates['problem_check'].copy()
        problem_data = event['event']
        problem_data['timestamp'] = self.DEFAULT_TIMESTAMP
        problem_data['username'] = 'test_user'
        problem_data['context'] = event['context']
        return (self.DEFAULT_TIMESTAMP, problem_data)

    def test_correct_problem_check(self):
        template = self.event_templates['problem_check']
        template['event']['success'] = 'correct'

        self.assert_single_map_output_load_jsons(
            json.dumps(template),
            self.get_expected_output_key(),
            self.get_expected_output_value_dict(),
        )

    def test_missing_problem_id(self):
        template = self.event_templates['problem_check']
        del template['event']['problem_id']
        self.assert_no_map_output_for(self.create_event_log_line(template=template))


class LatestProblemResponseTaskMapLegacyKeysTest(InitializeLegacyKeysMixin, LatestProblemResponseTaskMapTest):
    """Also test with legacy keys"""
    pass


@ddt
class LatestProblemResponseTaskReducerTest(ProblemResponseTestMixin, ReducerTestMixin, TestCase):
    """
    Tests the latest problem response reducer.
    """
    def setUp(self):
        super(LatestProblemResponseTaskReducerTest, self).setUp()

        self.initialize_ids()
        self.reduce_key = (self.course_id, self.problem_id, self.USERNAME)
        self.problem = 'Why is the sky blue?'
        self.question = 'Please chose the best answer to the question: Why is the sky blue?'
        # Construct a list of possible attempts to the problem
        # * answer: answer text, or list of answers, as provided in tracking logs.
        # * expected_answer: formatted answer text expected from reduce task.
        #   Correct answers are marked with [x], incorrect with [ ], and unknown with [-]
        #   Lists of answers are concatenated.
        # * correct: True, False, or None
        self.attempts = [
            dict(answer=('Ions in the atmosphere',),
                 correct=True),
            dict(answer='Reflection off\nthe    ocean',
                 expected_answer=('Reflection off the ocean',),
                 correct=False),
            dict(answer='Refraction of light <choicehint title="this is a multiline,\n'
                        r'empty choicehint."\>',
                 expected_answer=('Refraction of light ',),
                 correct=True),
            dict(answer='Little dancing fairies <choicehint label="incorrect">This is a\n'
                        'multiline choicehint</choicehint>',
                 expected_answer=('Little dancing fairies ',),
                 correct=False),
            dict(answer=('Ions in the atmosphere', 'Refraction of light'),
                 correct=True),
            dict(answer=('Little dancing fairies <choicehint>What if fairies were ions?</choicehint>',
                         'Refraction of light <choicehint selected="" title="Off fairies!" \\>'),
                 expected_answer=('Little dancing fairies ', 'Refraction of light '),
                 correct=False),
            dict(answer='It isn\'t blue.',
                 expected_answer=('It isn\'t blue.',),
                 correct=None),
        ]

    def create_input_output(self, attempts):
        """Returns an array of input problem attempts, and the expected output tuple."""

        # Incremented as we loop through the attempts
        total_attempts = 0
        first_attempt_date = None
        latest_attempt = {}

        inputs = []
        for idx, attempt in enumerate(attempts):
            answer_id = 'answer_{}'.format(idx)
            time = '2013-01-01 00:{0:02d}:00.0'.format(idx)
            correctness = None
            if attempt['correct'] is None:
                correctness = 'unknown'
            elif attempt['correct']:
                correctness = 'correct'
            else:
                correctness = 'incorrect'

            # Append the problem data to the inputs list
            problem_data = {
                'username': self.USERNAME,
                'context': {
                    'course_id': self.course_id,
                    'module': {
                        'display_name': self.problem,
                    },
                },
                'problem_id': self.problem_id,
                'attempts': idx + 1,
                'submission': {
                    answer_id: {
                        'question': self.question,
                    }
                },
                'answers': {
                    answer_id: attempt['answer'],
                },
                'correct_map': {
                    answer_id: {
                        'correctness': correctness,
                    }
                },
                'grade': 0,
                'max_grade': 1,
                'time': time,
            }
            inputs.append((time, json.dumps(problem_data)))

            # Update the summary data, and keep track of the "latest" attempt
            total_attempts += 1
            if not first_attempt_date or time < first_attempt_date:
                first_attempt_date = time
            if not latest_attempt or time > latest_attempt['last_attempt_date']:
                latest_attempt = attempt.copy()
                latest_attempt.update(dict(
                    answer_id=answer_id,
                    last_attempt_date=time,
                ))

        # Construct the expected problem response record
        date_field = DateTimeField()
        expected = ProblemResponseRecord(
            course_id=self.course_id,
            answer_id=latest_attempt['answer_id'],
            username=self.USERNAME,
            problem_id=self.problem_id,
            problem=self.problem,
            question=self.question,
            score=0,
            max_score=1,
            correct=latest_attempt['correct'],
            answer=latest_attempt.get('expected_answer', latest_attempt.get('answer')),
            total_attempts=total_attempts,
            first_attempt_date=date_field.deserialize_from_string(first_attempt_date),
            last_attempt_date=date_field.deserialize_from_string(latest_attempt['last_attempt_date']),
            location='',
            sort_idx=0,
        )
        # randomize the inputs, because their order shouldn't matter.
        random.shuffle(inputs)
        return (inputs, (expected.to_string_tuple(),))

    def test_no_events(self):
        self.assert_no_output([])

    @data(
        (0, None),  # all attempts, processed together
        (0, 1),  # each attempt, processed one by one
        (1, 2),
        (2, 3),
        (3, 4),
        (4, 5),
        (5, 6),
        (1, 3),  # and some other attempt subsets
        (2, 5),
        (3, None),
    )
    @unpack
    def test_problem_response(self, first_attempt, last_attempt):
        inputs, expected = self.create_input_output(self.attempts[first_attempt:last_attempt])
        self._check_output_complete_tuple(inputs, expected)


class LatestProblemResponseTaskReducerLegacyKeysTest(InitializeLegacyKeysMixin, LatestProblemResponseTaskReducerTest):
    """Also test with legacy keys"""
    pass


@ddt
class LatestProblemResponseDataTaskTest(ProblemResponseTestMixin, ReducerTestMixin, TestCase):
    """Test the properties of the LatestProblemResponseDataTask."""
    task_class = LatestProblemResponseDataTask
    DATE = '2013-12-17'

    def create_task(self, *args, **kwargs):
        """Initialize the test task with the given kwargs."""
        if 'output_root' not in kwargs:
            kwargs['output_root'] = self.output_dir
        if 'interval' not in kwargs:
            kwargs['interval'] = luigi.DateIntervalParameter().parse(self.DATE),
        self.task = self.task_class(  # pylint: disable=not-callable
            *args, **kwargs
        )

    def test_complete(self):
        self.create_task()
        self.assertFalse(self.task.complete())

        # Create the output_root/_SUCCESS file
        with open(os.path.join(self.output_dir, '_SUCCESS'), 'w') as success:
            success.write('')
        self.assertTrue(self.task.output().exists())
        self.assertTrue(self.task.complete())

    def test_extra_modules(self):
        import html5lib
        self.assertIn(html5lib, self.task.extra_modules())

    @data(
        (r'(?:<choicehint.*?</choicehint>)|(?:<choicehint.*?\>)', 'Choice ', 'Choice '),
        (r'(?:<choicehint.*?</choicehint>)|(?:<choicehint.*?\>)', 'Choice <choicehint>Hint</choicehint>', 'Choice '),
        (r'(?:<choicehint.*?</choicehint>)|(?:<choicehint.*?\>)', "Choice  \t\n\r<choicehint>Hint</choicehint>",
         'Choice '),
        (None, 'Choice <choicehint>Hint</choicehint>', 'Choice <choicehint>Hint</choicehint>'),
    )
    @unpack
    def test_clean_text_regex(self, clean_text_regex, input_str, expected_str):
        if isinstance(clean_text_regex, basestring):
            regex = re.compile(clean_text_regex)
        else:
            regex = clean_text_regex
        self.create_task(clean_text_regex=regex)

        self.assertEquals(
            self.task._clean_string(input_str),  # pylint: disable=protected-access
            expected_str)
        self.assertEquals(
            self.task._clean_string([input_str]),  # pylint: disable=protected-access
            (expected_str,))
        self.assertEquals(
            self.task._clean_string((input_str,)),  # pylint: disable=protected-access
            (expected_str,))


class LatestProblemResponseTableTaskTest(ReducerTestMixin, TestCase):
    """Test the properties of the LatestProblemResponseTableTask."""
    task_class = LatestProblemResponseTableTask

    def setUp(self):
        self.task = self.task_class()  # pylint: disable=not-callable

    def test_partition_by(self):
        self.assertEquals(self.task.partition_by, 'dt')

    def test_table_name(self):
        self.assertEquals(self.task.table, 'problem_response_latest')

    def test_columns(self):
        # Ensure the "key" columns are present and in the required order
        columns = self.task.columns
        self.assertEquals(columns[0], ('course_id', 'STRING'))
        self.assertEquals(columns[1], ('answer_id', 'STRING'))


class ProblemResponseReportInputTask(luigi.Task):
    """Use for the ProblemResponseReportTask.input_task parameter."""
    output_root = luigi.Parameter()


@ddt
class ProblemResponseReportTaskMapTest(MapperTestMixin, TestCase):
    """Test the properties of the ProblemResponseReportTask."""
    task_class = ProblemResponseReportTask
    DEFAULT_ARGS = dict(
        input_task=ProblemResponseReportInputTask(output_root='/tmp/input'),
        output_root='/tmp/output',
    )

    @data(
        (''),
        ('foo'),
        ('foo   '),
        ('foo bar'),
        ('foo    bar    baz   qux'),
    )
    def test_invalid_input(self, line):
        self.assert_no_map_output_for(line)

    @data(
        ("foo\t", [('foo', ('foo', ''))]),
        ("foo\tbar", [('foo', ('foo', 'bar'))]),
        ("foo\tbar\tbaz\tqux", [('foo', ('foo', 'bar', 'baz', 'qux'))]),
    )
    @unpack
    def test_valid_input(self, line, expected):
        self.assert_map_output(line, expected)

    def test_output_path_for_key(self):
        filename = self.task.output_path_for_key('my:course:id')
        self.assertEquals(filename, '/tmp/output/my_course_id_problem_response.csv')


class ProblemResponseReportTestMixin(ProblemResponseTestMixin, TestCase):
    """
    Helper methods for testing the problem response report tasks.
    """
    course_id = 'course_{}'

    def create_input_output(self, num_courses, num_responses):
        """
        Create a set of inputs and outputs for the given number of courses and responses.

        Both the returned 'inputs' and 'expected' dicts are keyed by course_id.
        """
        inputs = {}
        expected = {}
        report_fields = ProblemResponseRecord.get_fields().keys()

        idx = 0
        for course_idx in range(num_courses):
            course_id = self.course_id.format(course_idx)
            for _ in range(num_responses):
                idx += 1
                input_row = (
                    course_id,
                    'answer_{}'.format(idx),
                    'problem_{}'.format(idx),
                    'user_{}'.format(idx),
                    'problem {}'.format(idx),
                    'question {}'.format(idx),
                    '4.0',
                    '10.0',
                    'False',
                    'answer {}'.format(idx),
                    '20',
                    self.DATE,
                    self.DATE,
                    '',
                    '{}'.format(idx),
                )

                # First row is the header: field names
                if course_id not in expected:
                    inputs[course_id] = []
                    expected[course_id] = [
                        ','.join(report_fields),
                    ]

                inputs[course_id].append(input_row)
                expected[course_id].append(','.join(input_row))

        return inputs, expected

    def assert_report_files_correct(self, expected):
        """Ensure the number of output files created is correct."""
        output_files = [fn for fn in os.listdir(self.output_dir)
                        if os.path.isfile(os.path.join(self.output_dir, fn))]
        self.assertEquals(len(output_files), len(expected.keys()))

        # Ensure each output file contains the correct data
        for course_id in expected.keys():
            filename = os.path.join(self.output_dir, '{}_response.csv'.format(course_id))
            with open(filename) as course_report_file:
                num_lines = 0
                for line in course_report_file:
                    self.assertEquals(line.rstrip(), expected[course_id][num_lines])
                    num_lines += 1
                self.assertEquals(num_lines, len(expected[course_id]))


@ddt
class ProblemResponseReportTaskReducerTest(ReducerTestMixin, ProblemResponseReportTestMixin):
    """
    Tests the problem response report reducer.
    """
    task_class = ProblemResponseReportTask
    DATE = '2013-07-03 00:00:00.000000'

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(  # pylint: disable=invalid-name
            input_task=ProblemResponseReportInputTask(output_root=self.input_dir),
            output_root=self.output_dir,
            report_filename_template='{course_id}_response.csv',
            report_fields=None,
            report_field_datetime_format=DateTimeField.string_format,
            report_field_list_delimiter='"|"',
        )
        super(ProblemResponseReportTaskReducerTest, self).setUp()

    def test_no_events(self):
        self.assert_no_output([])

    @data(
        (0, 100),
        (1, 1),
        (10, 100),
    )
    @unpack
    def test_multi_file_reduce(self, num_courses, num_responses):
        inputs, expected = self.create_input_output(num_courses, num_responses)

        # MultiOutputMapReduceJobTasks.reduce() returns an empty tuple
        for course_id in expected.keys():
            self.reduce_key = course_id
            outputs = self._get_reducer_output(inputs[course_id])
            self.assertEquals(outputs, tuple())

        self.assert_report_files_correct(expected)

    def test_init_report_fields(self):
        self.DEFAULT_ARGS['report_fields'] = '["course_id", "name"]'  # use JSON string
        self.create_task()

        test_record_class = namedtuple('TestRecord', 'course_id name ignored_field')
        record = test_record_class(course_id='course-1', name='Course Name', ignored_field='more data')
        self.assertEquals(self.task._record_to_string_dict(record),  # pylint: disable=protected-access
                          dict(course_id='course-1', name='Course Name'))

    def test_report_fields(self):
        self.task.report_fields = ['course_id']
        test_record_class = namedtuple('TestRecord', 'course_id ignored_field1  ignored_field2')
        record = test_record_class(course_id='course-1', ignored_field1='data', ignored_field2='some more data')
        self.assertEquals(self.task._record_to_string_dict(record),  # pylint: disable=protected-access
                          dict(course_id='course-1'))

    @data(
        (None, '2016-10-12 12:43:13.235134'),
        ('%Y-%m-%d', '2016-10-12'),
    )
    @unpack
    def test_report_field_datetime_format(self, datetime_format, date_string):
        self.task.report_fields = ['date_field']
        self.task.report_field_datetime_format = datetime_format
        test_record_class = namedtuple('TestRecord', 'date_field')
        record = test_record_class(date_field=datetime(2016, 10, 12, 12, 43, 13, 235134))
        self.assertEquals(self.task._record_to_string_dict(record),  # pylint: disable=protected-access
                          dict(date_field=date_string))

    def test_report_field_list_delimiter(self):
        self.task.report_fields = ['list_field']
        self.task.report_field_list_delimiter = None
        test_record_class = namedtuple('TestRecord', 'list_field')
        record = test_record_class(list_field=['a', 'b'])
        self.assertEquals(self.task._record_to_string_dict(record),  # pylint: disable=protected-access
                          dict(list_field="['a', 'b']"))
