"""Utility mixins that simplify tests for map reduce jobs."""
import json
import datetime

import luigi


class MapperTestMixin(object):
    """
    Base class for map function tests.

    Assumes that self.task_class is defined in a derived class.
    """

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"
    # This dictionary stores the default values for arguments to various task constructors; if not told otherwise,
    # the task constructor will pull needed values from this dictionary.
    DEFAULT_ARGS = {
        'interval': DEFAULT_DATE,
        'output_root': '/fake/output',
        'end_date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
        'import_date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
        'geolocation_data': 'test://data/data.file',
        'mapreduce_engine': 'local',
        'user_country_output': 'test://output/',
        'name': 'test',
        'src': ['test://input/'],
        'dest': 'test://output/'
    }

    task_class = None

    def setUp(self):
        self.event_templates = {}
        self.default_event_template = ''
        if hasattr(self, 'interval') and self.interval is not None:
            self.create_task(interval=self.interval)
        else:
            self.create_task()

    def create_task(self, **kwargs):
        """Allow arguments to be passed to the task constructor."""

        new_kwargs = {}
        for attr in self.DEFAULT_ARGS:
            if not hasattr(self.task_class, attr):
                continue
            value = kwargs.get(attr, self.DEFAULT_ARGS.get(attr))
            if attr == 'interval':
                new_kwargs[attr] = luigi.DateIntervalParameter().parse(value)
            else:
                new_kwargs[attr] = value

        self.task = self.task_class(**new_kwargs)  # pylint: disable=not-callable
        self.task.init_local()

    def create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self.create_event_dict(**kwargs))

    def create_event_dict(self, **kwargs):
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

    def assert_single_map_output_load_jsons(self, line, expected_key, expected_value):
        """
        Checks if two tuples are equal, but loading jsons and comparing dictionaries rather than comparing JSON strings
        directly to avoid potential ordering issues.

        args:
            line is a tuple, possibly including json strings.
            expected_key is the expected key of the result of the mapping.
            expected_value is a tuple, possibly including dictionaries to be compared with the json strings in values.
        """
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        actual_key, actual_value = row
        self.assertEquals(actual_key, expected_key)

        read_list = []
        expected_list = list(expected_value)
        for element in actual_value:
            # Load the json if we can, otherwise, just put in the element.
            try:
                read_list.append(json.loads(element))
            except ValueError:
                read_list.append(element)
        self.assertEquals(read_list, expected_list)

    def assert_no_map_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(
            tuple(self.task.mapper(line)),
            tuple()
        )


class ReducerTestMixin(object):
    """
    Base class for reduce function tests.

    Assumes that self.task_class is defined in a derived class.
    """

    DATE = '2013-12-17'
    COURSE_ID = 'foo/bar/baz'
    USERNAME = 'test_user'
    # This dictionary stores the default values for arguments to various task constructors; if not told otherwise,
    # the task constructor will pull needed values from this dictionary.
    DEFAULT_ARGS = {
        'interval': DATE,
        'output_root': '/fake/output',
        'end_date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
        'import_date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
        'geolocation_data': 'test://data/data.file',
        'mapreduce_engine': 'local',
        'user_country_output': 'test://output/',
        'name': 'test',
        'src': ['test://input/'],
        'dest': 'test://output/'
    }

    reduce_key = tuple()
    task_class = None

    def setUp(self):

        new_kwargs = {}
        for attr in self.DEFAULT_ARGS:
            if not hasattr(self.task_class, attr):
                continue
            value = getattr(self, attr, self.DEFAULT_ARGS.get(attr))
            if attr == 'interval':
                new_kwargs[attr] = luigi.DateIntervalParameter().parse(value)
            else:
                new_kwargs[attr] = value

        self.task = self.task_class(**new_kwargs)  # pylint: disable=not-callable

        self.task.init_local()
        self.reduce_key = tuple()

    def assert_no_output(self, input_value):
        """Asserts that the given input produces no output."""
        output = self._get_reducer_output(input_value)
        self.assertEquals(len(output), 0)

    def _get_reducer_output(self, inputs):
        """Runs the reducer and return the output."""
        return tuple(self.task.reducer(self.reduce_key, inputs))

    def _check_output_complete_tuple(self, inputs, expected):
        """Compare generated with expected output, comparing the entire tuples.

        args:
            inputs is a valid input to the subclass's reducer.
            expected is a tuple containing the expected output of the reducer.
        """
        self.assertEquals(self._get_reducer_output(inputs), expected)

    def _check_output_by_key(self, inputs, column_values):
        """
        Compare generated with expected output, but only checking specified columns

        args:
            inputs is a valid input to the subclass's reducer.
            column_values is a list of dictionaries, where the (key, value) pairs in the dictionary correspond to (column_num, expected_value)
                pairs in the expected reducer output.
        """
        output = self._get_reducer_output(inputs)
        if not isinstance(column_values, list):
            column_values = [column_values]
        self.assertEquals(len(output), len(column_values), '{0} != {1}'.format(output, column_values))
        for output_tuple, expected_columns in zip(output, column_values):
            for column_num, expected_value in expected_columns.iteritems():
                self.assertEquals(output_tuple[column_num], expected_value)

    def _check_output_tuple_with_key(self, inputs, expected):
        """
        Compare generated with expected output, checking the whole tuple and including keys in the expected
        (in case the reduce_key changes midway through).

        args:
            inputs is a valid input to the subclass's reducer.
            expected is an iterable of (key, value) pairs corresponding to expected output.
        """
        expected_with_key = tuple([(key, self.reduce_key + value) for key, value in expected])
        self.assertEquals(self._get_reducer_output(inputs), expected_with_key)
