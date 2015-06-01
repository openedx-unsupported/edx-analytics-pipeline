"""Utility mixins that simplify tests for map reduce jobs."""
import inspect
import json
import datetime

import luigi
from luigi.date_interval import Year
from edx.analytics.tasks.location_per_course import LastCountryOfUser
from edx.analytics.tasks.user_location import LastCountryForEachUser


class MapperTestMixin(object):
    """
    Base class for map function tests.

    Assumes that self.task_class is defined in a derived class.
    """

    DEFAULT_USER_ID = 10
    DEFAULT_TIMESTAMP = "2013-12-17T15:38:32.805444"
    DEFAULT_DATE = "2013-12-17"

    task_class = None

    def setUp(self):
        self.event_templates = {}
        self.default_event_template = ''
        if hasattr(self, 'interval') and self.interval is not None:
            self.create_task(interval=self.interval)
        else:
            self.create_task()

    def create_task(self, interval=None):
        """Allow arguments to be passed to the task constructor."""
        if not interval:
            interval = self.DEFAULT_DATE
        #LastCountryForEachUser is nonstandard (and slated for deprecation),
        #not using the same interval and output_root args as the other classes,
        #so we have a separate task initializer for it
        if hasattr(self.task_class, 'interval') and hasattr(self.task_class, 'output_root'):
            self.task = self.task_class(
                interval=luigi.DateIntervalParameter().parse(interval),
                output_root='/fake/output',
            )
        elif hasattr(self, 'interval'):
            self.task = self.task_class(
                interval=luigi.DateIntervalParameter().parse(interval),
                output_root='/fake/output',
            )
        elif self.task_class == LastCountryForEachUser:
            self.task = self.task_class(name='test', src=['test://input/'], dest='test://output/',
                  end_date=datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
                  geolocation_data='test://data/data.file')
        elif self.task_class == LastCountryOfUser:
                    self.task = self.task_class(
            mapreduce_engine='local',
            user_country_output='test://output/',
            interval=Year.parse('2013'),
        )

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

    #TODO: talk about this issue with someone
    def assert_single_map_output_weak(self, line, expected_key, expected_value):
        """Assert that an input line generates exactly one output record with the expected key and value, but compares
        dicts rather than JSON strings to avoid issues of inconsistent ordering arising from json.dumps()"""
        mapper_output = tuple(self.task.mapper(line))
        self.assertEquals(len(mapper_output), 1)
        row = mapper_output[0]
        self.assertEquals(len(row), 2)
        actual_key, actual_value = row
        self.assertEquals(expected_key, actual_key)
        #actual_info = mapper_output[0][1][1]
        actual_data = json.loads(actual_value[1])
        self.assertEquals(actual_data, expected_value)


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


    reduce_key = tuple()
    task_class = None

    def setUp(self):
        if hasattr(self.task_class, 'interval') and hasattr(self.task_class, 'output_root'):
            if (not hasattr(self, 'interval')) or self.interval is None:
                self.task = self.task_class(
                    interval=luigi.DateIntervalParameter().parse('2013-12-17'),
                    output_root='/fake/output',
                )
            else: #if we have a predifined interval already, use it
                self.task = self.task_class(
                    interval=luigi.DateIntervalParameter().parse(self.interval),
                    output_root='/fake/output',
                )
        elif self.task_class == LastCountryForEachUser:
            self.task = self.task_class(name='test', src=['test://input/'], dest='test://output/',
                  end_date=datetime.datetime.strptime('2014-04-01', '%Y-%m-%d').date(),
                  geolocation_data='test://data/data.file')
        elif self.task_class == LastCountryOfUser:
            self.task = self.task_class(
                mapreduce_engine='local',
                user_country_output='test://output/',
                interval=Year.parse('2014'),
            )
        else: #TODO: temporary fix for tiny test class at the end of test_course_enroll
            self.task = self.task_class()
            return

        self.task.init_local()
        self.reduce_key = tuple()

    def assert_no_output(self, input_value):
        """Asserts that the given input produces no output."""
        output = self._get_reducer_output(input_value)
        self.assertEquals(len(output), 0)

    def _get_reducer_output(self, inputs):
        """Run the reducer and return the output"""
        return tuple(self.task.reducer(self.reduce_key, inputs))

    def _check_output_complete_tuple(self, inputs, expected):
        """Compare generated with expected output, comparing the entire tuples

        args:
            inputs is a valid input to the subclass's reducer
            expected is a tuple containing the expected output of the reducer
        """
        self.assertEquals(self._get_reducer_output(inputs), expected)

    def _check_output_by_key(self, inputs, column_values):
        """
        Compare generated with expected output, but only checking specified columns

        args:
            inputs is a valid input to the subclass's reducer
            column_values is a list of dictionaries, where the (key, value) pairs in the dictionary correspond to (column_num, expected_value)
                pairs in the expected reducer output
        """
        output = self._get_reducer_output(inputs)
        if not isinstance(column_values, list):
            column_values = [column_values]
        self.assertEquals(len(output), len(column_values), '{0} != {1}'.format(output, column_values))
        for output_tuple, expected_columns in zip(output, column_values):
            for column_num, expected_value in expected_columns.iteritems():
                self.assertEquals(output_tuple[column_num], expected_value)

    def _check_output_tuple_with_key(self, inputs, expected):
        """Compare generated with expected output, checking the whole tuple and including keys in the expected (in case the reduce_key changes
            midway through

        args:
            inputs is a valid input to the subclass's reducer
            expected is an iterable of (key, value) pairs corresponding to expected output
        """
        expected_with_key = tuple([(key, self.reduce_key + value) for key, value in expected])
        self.assertEquals(self._get_reducer_output(inputs), expected_with_key)

