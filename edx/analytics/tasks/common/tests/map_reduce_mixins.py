"""Utility mixins that simplify tests for map reduce jobs."""
import datetime
import json

import luigi
import luigi.task


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
        'date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
        'import_date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
        'geolocation_data': 'test://data/data.file',
        'mapreduce_engine': 'local',
        'user_country_output': 'test://output/',
        'name': 'test',
        'src': '["test://input/"]',
        'dest': 'test://output/',
        'overwrite_from_date': datetime.date(2014, 4, 1),
    }

    task_class = None

    def setUp(self):
        luigi.task.Register.clear_instance_cache()
        self.event_templates = {}
        self.default_event_template = ''
        if hasattr(self, 'interval') and self.interval is not None:
            self.create_task(interval=self.interval)
        else:
            self.create_task()

    def create_task(self, **kwargs):
        """Allow arguments to be passed to the task constructor."""

        new_kwargs = {}

        merged_arguments = self.DEFAULT_ARGS.copy()
        merged_arguments.update(kwargs)

        for attr in merged_arguments:
            if getattr(self.task_class, attr, None) is None:
                continue
            value = merged_arguments.get(attr)
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
        template_name = kwargs.pop('template_name', self.default_event_template)
        event_dict = kwargs.pop('template', self.event_templates[template_name]).copy()
        event_dict.update(**kwargs)
        return event_dict

    def assert_single_map_output(self, line, expected_key, expected_value):
        """Assert that an input line generates exactly one output record with the expected key and value"""
        self.assert_map_output(line, [(expected_key, expected_value)])

    def assert_map_output(self, line, expected_key_value_pairs):
        """Assert that an input line generates the expected key value pairs"""
        mapper_output = tuple(self.task.mapper(line))
        mapper_output_list = list(mapper_output)
        self.assertEqual(len(mapper_output_list), len(expected_key_value_pairs))
        for i, row in enumerate(mapper_output):
            self.assertEquals(len(row), 2)
            actual_key, actual_value = row
            self.assertEquals(expected_key_value_pairs[i][0], actual_key)
            self.assertEquals(expected_key_value_pairs[i][1], actual_value)

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
        'src': '["test://input/"]',
        'dest': 'test://output/',
        'date': datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
        'overwrite_from_date': datetime.date(2014, 4, 1),
    }

    reduce_key = tuple()
    task_class = None

    def setUp(self):
        luigi.task.Register.clear_instance_cache()
        self.create_task()
        self.reduce_key = tuple()

    def get_default_task_args(self):
        """Some reasonable defaults for common task parameters."""
        new_kwargs = {}
        for attr in self.DEFAULT_ARGS:
            if hasattr(self.task_class, 'get_params'):
                task_param_names = frozenset([param[0] for param in self.task_class.get_params()])
                if attr not in task_param_names:
                    continue
            elif not hasattr(self.task_class, attr):
                continue
            value = getattr(self, attr, self.DEFAULT_ARGS.get(attr))
            if attr == 'interval':
                new_kwargs[attr] = luigi.DateIntervalParameter().parse(value)
            else:
                new_kwargs[attr] = value
        return new_kwargs

    def create_task(self, **kwargs):
        """Create a new instance of the class under test and assign it to `self.task`."""
        new_kwargs = self.get_default_task_args()
        new_kwargs.update(kwargs)
        self.task = self.task_class(**new_kwargs)  # pylint: disable=not-callable
        self.task.init_local()

    def assert_no_output(self, input_value):
        """Asserts that the given input produces no output."""
        output = self._get_reducer_output(input_value)
        self.assertEquals(len(output), 0, 'Expected no output, found {}'.format(output))

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
            column_values is a list of dictionaries, where the (key, value) pairs in the dictionary correspond to
                (column_num, expected_value) pairs in the expected reducer output.
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

    def _check_output_by_record_field(self, inputs, field_values):
        """
        Compare generated with expected output, but only checking specified columns

        args:
            inputs is a valid input to the subclass's reducer.
            column_values is a list of dictionaries, where the (key, value) pairs in the dictionary correspond to
                (column_num, expected_value) pairs in the expected reducer output.
        """
        self.assertTrue(getattr(self, 'output_record_type', None) is not None)
        field_positions = {k: i for i, k in enumerate(self.output_record_type.get_fields())}
        output = self._get_reducer_output(inputs)
        if not isinstance(field_values, list):
            field_values = [field_values]
        self.assertEquals(len(output), len(field_values), '{0} != {1}'.format(output, field_values))
        for output_tuple, expected_field_values in zip(output, field_values):
            for field_name, expected_value in expected_field_values.iteritems():
                self.assertEquals(output_tuple[field_positions[field_name]], expected_value)
