"""
Tests for geolocation-per-course tasks.
"""

import json
import textwrap
from unittest import TestCase

from luigi.date_interval import Year
from luigi.parameter import DateIntervalParameter, DateParameter, MissingParameterException
from mock import Mock, call

from edx.analytics.tasks.common.pathutil import PathSelectionByDateIntervalTask
from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.insights.location_per_course import (
    InsertToMysqlLastCountryPerCourseTask, LastCountryOfUser, LastCountryOfUserPartitionTask,
    LastDailyIpAddressOfUserTask, QueryLastCountryPerCourseTask
)
from edx.analytics.tasks.util.geolocation import UNKNOWN_CODE, UNKNOWN_COUNTRY
from edx.analytics.tasks.util.tests.test_geolocation import FakeGeoLocation


class LastCountryOfUserTaskParamTest(TestCase):

    def test_use_interval(self):
        interval = DateIntervalParameter().parse('2013-01-01')
        interval_start = None
        LastCountryOfUser(
            interval=interval,
            interval_start=interval_start,
        )

    def test_use_interval_start(self):
        interval = None
        interval_start = DateParameter().parse('2013-01-01')
        LastCountryOfUser(
            interval=interval,
            interval_start=interval_start,
        )

    def test_missing_interval(self):
        interval = None
        interval_start = None
        with self.assertRaises(MissingParameterException):
            LastCountryOfUser(
                interval=interval,
                interval_start=interval_start,
            )


class LastDailyIpAddressOfUserMapperTestCase(MapperTestMixin, TestCase):
    """Tests of LastDailyIpAddressOfUserTask.mapper()"""

    user_id = 1
    timestamp = "2013-12-17T15:38:32.805444"
    ip_address = FakeGeoLocation.ip_address_1

    def setUp(self):
        self.task_class = LastDailyIpAddressOfUserTask
        super(LastDailyIpAddressOfUserMapperTestCase, self).setUp()

    def _create_event_log_line(self, **kwargs):
        """Create an event log with test values, as a JSON string."""
        return json.dumps(self._create_event_dict(**kwargs))

    def _create_event_dict(self, **kwargs):
        """Create an event log with test values, as a dict."""
        # Define default values for event log entry.
        event_dict = {
            "time": "{0}+00:00".format(self.timestamp),
            "ip": self.ip_address,
            "context": {
                "user_id": self.user_id,
            }
        }
        event_dict.update(**kwargs)
        return event_dict

    def test_non_enrollment_event(self):
        line = 'this is garbage'
        self.assert_no_map_output_for(line)

    def test_bad_datetime(self):
        line = self._create_event_log_line(time='this is a bogus time')
        self.assert_no_map_output_for(line)

    def test_after_end_date(self):
        line = self._create_event_log_line(time="2015-12-17T15:38:32.805444")
        self.assert_no_map_output_for(line)

    def test_missing_context(self):
        event_dict = self._create_event_dict()
        del event_dict['context']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_missing_user_id(self):
        line = self._create_event_log_line(context={'user_id': ''})
        self.assert_no_map_output_for(line)

    def test_missing_ip_address(self):
        event_dict = self._create_event_dict()
        del event_dict['ip']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_good_event(self):
        line = self._create_event_log_line()
        self.assert_single_map_output(line, "2013-12-17", (self.timestamp, self.ip_address, None, self.user_id))


class LastDailyIpAddressOfUserReducerTestCase(ReducerTestMixin, TestCase):
    """Tests of LastDailyIpAddressOfUserTask.reducer()"""

    user_id = 1
    timestamp = '2013-12-17T15:38:32.805444'
    earlier_timestamp = '2013-12-15T15:38:32.805444'
    ip_address = FakeGeoLocation.ip_address_1
    earlier_ip_address = FakeGeoLocation.ip_address_2
    course_id = 'DummyX/Course/ID'

    def setUp(self):
        self.task_class = LastDailyIpAddressOfUserTask
        super(LastDailyIpAddressOfUserReducerTestCase, self).setUp()

    def test_multi_output_reducer(self):
        # To test sorting, the first sample is made to sort after the
        # second sample.
        input_1 = (self.timestamp, self.ip_address, self.course_id, self.user_id)
        input_2 = (self.earlier_timestamp, self.earlier_ip_address, self.course_id, self.user_id)

        mock_output_file = Mock()
        self.task.multi_output_reducer('random_date', [input_1, input_2], mock_output_file)
        self.assertEquals(len(mock_output_file.write.mock_calls), 2)
        expected_string = '\t'.join([self.timestamp, self.ip_address, str(self.user_id), self.course_id])
        self.assertEquals(mock_output_file.write.mock_calls[0], call(expected_string))
        self.assertEquals(mock_output_file.write.mock_calls[1], call('\n'))

    def test_output_path(self):
        output_path = self.task.output_path_for_key(self.DATE)
        expected_output_path = 's3://fake/warehouse/last_ip_of_user_id/dt={0}/last_ip_of_user_{0}'.format(self.DATE)
        self.assertEquals(output_path, expected_output_path)
        tasks = self.task.downstream_input_tasks()
        self.assertEquals(len(tasks), 1)
        self.assertEquals(tasks[0].url, expected_output_path)


class LastCountryOfUserMapperTestCase(MapperTestMixin, TestCase):
    """Tests of LastCountryOfUser.mapper()"""

    user_id = 1
    timestamp = '2013-12-17T15:38:32.805444'
    ip_address = FakeGeoLocation.ip_address_1
    course_id = 'DummyX/Course/ID'

    def setUp(self):
        self.task_class = LastCountryOfUser
        super(LastCountryOfUserMapperTestCase, self).setUp()

    def test_mapper(self):
        line = '\t'.join([self.timestamp, self.ip_address, str(self.user_id), self.course_id])
        self.assert_single_map_output(line, self.user_id, (self.timestamp, self.ip_address))

    def test_requires_local(self):
        tasks = self.task.requires_local()
        self.assertEquals(len(tasks), 2)
        self.assertEquals(tasks['geolocation_data'].url, 'test://data/data.file')
        self.assertTrue(isinstance(tasks['user_addresses_task'], LastDailyIpAddressOfUserTask))

    def test_requires_hadoop(self):
        tasks = self.task.requires_hadoop()
        self.assertEquals(len(tasks), 2)
        self.assertTrue(isinstance(tasks['path_selection_task'], PathSelectionByDateIntervalTask))
        self.assertEquals(len(tasks['downstream_input_tasks']), 14)


class LastCountryOfUserReducerTestCase(ReducerTestMixin, TestCase):
    """Tests of LastCountryOfUser.reducer()"""

    def setUp(self):
        self.task_class = LastCountryOfUser
        super(LastCountryOfUserReducerTestCase, self).setUp()

        self.user_id = 1
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.earlier_timestamp = "2013-12-15T15:38:32.805444"
        self.task.geoip = FakeGeoLocation()
        self.reduce_key = self.user_id

    def test_no_ip(self):
        self.assert_no_output([])

    def test_single_ip(self):
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_ip(self):
        inputs = [
            (self.earlier_timestamp, FakeGeoLocation.ip_address_1),
            (self.timestamp, FakeGeoLocation.ip_address_2),
        ]
        expected = (((FakeGeoLocation.country_name_2, FakeGeoLocation.country_code_2), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_ip_in_different_order(self):
        inputs = [
            (self.timestamp, FakeGeoLocation.ip_address_2),
            (self.earlier_timestamp, FakeGeoLocation.ip_address_1),
        ]
        expected = (((FakeGeoLocation.country_name_2, FakeGeoLocation.country_code_2), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_country_name_exception(self):
        self.task.geoip.country_name_by_addr = Mock(side_effect=Exception)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_country_code_exception(self):
        self.task.geoip.country_code_by_addr = Mock(side_effect=Exception)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_missing_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value=None)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_empty_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value="  ")
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_missing_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value=None)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_empty_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value="  ")
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)

    def test_other_user_id(self):
        self.user_id = 2
        self.reduce_key = self.user_id
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1), self.user_id),)
        self._check_output_complete_tuple(inputs, expected)


class LastCountryOfUserPartitionTestCase(TestCase):
    """Tests to validate LastCountryOfUserPartitionTask."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating LastCountryOfUserPartitionTask."""
        return {
            'interval': Year.parse('2013'),
        }

    def test_query_with_date_interval(self):
        task = LastCountryOfUserPartitionTask(**self._get_kwargs())
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;

            ALTER TABLE `last_country_of_user_id` ADD IF NOT EXISTS PARTITION (`dt`='2014-01-01');
            """
        )
        self.assertEquals(query, expected_query)

    def test_overwrite(self):
        kwargs = self._get_kwargs()
        kwargs['overwrite'] = True
        task = LastCountryOfUserPartitionTask(**kwargs)
        self.assertFalse(task.complete())

    def test_requires(self):
        task = LastCountryOfUserPartitionTask(**self._get_kwargs())
        required_task = list(task.requires())[0]
        self.assertEquals(required_task.output().path, 's3://fake/warehouse/last_country_of_user_id/dt=2014-01-01')


class QueryLastCountryPerCourseTaskTestCase(TestCase):
    """Tests to validate QueryLastCountryPerCourseTask."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating LastCountryOfUserPartitionTask."""
        return {
            'interval': Year.parse('2013'),
        }

    def test_query(self):
        task = QueryLastCountryPerCourseTask(**self._get_kwargs())
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS course_enrollment_location_current;
            CREATE EXTERNAL TABLE course_enrollment_location_current (
                `date` STRING,
                course_id STRING,
                country_code STRING,
                count INT,
                cumulative_count INT
            )
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION 's3://fake/warehouse/course_enrollment_location_current/';

            INSERT OVERWRITE TABLE course_enrollment_location_current
            SELECT
                sce.dt,
                sce.course_id,
                uc.country_code,
                sum(if(sce.is_active, 1, 0)),
                count(sce.user_id)
            FROM student_courseenrollment sce
            LEFT OUTER JOIN last_country_of_user_id uc on sce.user_id = uc.user_id
            GROUP BY sce.dt, sce.course_id, uc.country_code;
            """
        )
        self.assertEquals(query, expected_query)

    def test_output(self):
        task = QueryLastCountryPerCourseTask(**self._get_kwargs())
        self.assertEquals(task.output().path, 's3://fake/warehouse/course_enrollment_location_current')

    def test_requires(self):
        task = QueryLastCountryPerCourseTask(**self._get_kwargs())
        required_tasks = list(task.requires())
        self.assertEquals(len(required_tasks), 1)
        self.assertEquals(len(required_tasks[0]), 2)


class InsertToMysqlLastCountryPerCourseTaskTestCase(TestCase):
    """Tests to validate InsertToMysqlLastCountryPerCourseTask."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating InsertToMysqlLastCountryPerCourseTask."""
        return {
            'interval': Year.parse('2013'),
        }

    def test_requires(self):
        task = InsertToMysqlLastCountryPerCourseTask(**self._get_kwargs())
        required_tasks = dict(task.requires())
        self.assertEquals(len(required_tasks), 2)
        self.assertEquals(required_tasks['credentials'].output().path, 's3://fake/credentials.json')
        self.assertEquals(required_tasks['insert_source'].output().path, 's3://fake/warehouse/course_enrollment_location_current')

    def test_requires_with_overwrite(self):
        kwargs = self._get_kwargs()
        kwargs['overwrite'] = True
        task = InsertToMysqlLastCountryPerCourseTask(**kwargs)
        required_tasks = task.requires()
        query_task = required_tasks['insert_source']
        self.assertTrue(query_task.overwrite)
