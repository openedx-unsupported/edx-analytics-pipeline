"""Utility mixins that simplify tests for map reduce jobs."""
import json

import luigi


class MapperTestMixin(object):
    """Base class for map function tests"""

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    task_class = None

    def setUp(self):
        self.event_templates = {}
        self.default_event_template = ''
        self.create_task()

    def create_task(self, interval=None):
        """Allow arguments to be passed to the task constructor."""
        if not interval:
            interval = self.DEFAULT_DATE
        self.task = self.task_class(
            interval=luigi.DateIntervalParameter().parse(interval),
            output_root='/fake/output',
        )
        self.task.init_local()

    def create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        template_name = kwargs.get('template_name', self.default_event_template)
        event_dict = kwargs.pop('template', self.event_templates[template_name]).copy()
        event_dict.update(**kwargs)
        return event_dict

    def assert_single_map_output(self, line, expected_key, expected_value):
        """Assert that an input line generates exactly one output record with the expected key and value"""
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        actual_key, actual_value = row
        self.assertEquals(expected_key, actual_key)
        self.assertEquals(expected_value, actual_value)

    def assert_no_map_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(
            tuple(self.task.mapper(line)),
            tuple()
        )


class ReducerTestMixin(object):
    """Base class for reduce function tests"""

    DATE = '2013-12-17'
    COURSE_ID = 'foo/bar/baz'
    USERNAME = 'test_user'

    reduce_key = tuple()
    task_class = None

    def setUp(self):
        self.task = self.task_class(
            interval=luigi.DateIntervalParameter().parse(self.DATE),
            output_root='/fake/output',
        )
        self.task.init_local()
        self.reduce_key = tuple()

    def assert_no_output(self, input):
        output = self._get_reducer_output(input)
        self.assertEquals(len(output), 0)

    def _get_reducer_output(self, inputs):
        """Run the reducer and return the output"""
        return tuple(self.task.reducer(self.reduce_key, inputs))

    def _check_output(self, inputs, column_values):
        """Compare generated with expected output."""
        output = self._get_reducer_output(inputs)
        if not isinstance(column_values, list):
            column_values = [column_values]
        self.assertEquals(len(output), len(column_values), '{0} != {1}'.format(output, column_values))
        for output_tuple, expected_columns in zip(output, column_values):
            for column_num, expected_value in expected_columns.iteritems():
                self.assertEquals(output_tuple[column_num], expected_value)