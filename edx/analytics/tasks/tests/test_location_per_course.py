"""
Tests for geolocation-per-course tasks.
"""

import json
import textwrap

from mock import Mock, patch, call

from luigi.date_interval import Year

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.location_per_course import (
    LastDailyIpAddressOfUserTask,
    LastCountryOfUserPartitionTask,
    LastCountryOfUser,
    QueryLastCountryPerCourseTask,
    InsertToMysqlLastCountryPerCourseTask,
)
from edx.analytics.tasks.pathutil import PathSelectionByDateIntervalTask
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.util.geolocation import UNKNOWN_COUNTRY, UNKNOWN_CODE
from edx.analytics.tasks.util.tests.test_geolocation import FakeGeoLocation


class LastDailyIpAddressOfUserMapperTestCase(MapperTestMixin, unittest.TestCase):
    """Tests of LastDailyIpAddressOfUserTask.mapper()"""

    username = 'test_user'
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
            "username": self.username,
            "time": "{0}+00:00".format(self.timestamp),
            "ip": self.ip_address,
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

    def test_missing_username(self):
        event_dict = self._create_event_dict()
        del event_dict['username']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_missing_ip_address(self):
        event_dict = self._create_event_dict()
        del event_dict['ip']
        line = json.dumps(event_dict)
        self.assert_no_map_output_for(line)

    def test_good_event(self):
        line = self._create_event_log_line()
        self.assert_single_map_output(line, "2013-12-17", (self.timestamp, self.ip_address, None, self.username))

    def test_username_with_newline(self):
        line = self._create_event_log_line(username="baduser\n")
        self.assert_single_map_output(line, "2013-12-17", (self.timestamp, self.ip_address, None, "baduser"))


class LastDailyIpAddressOfUserReducerTestCase(ReducerTestMixin, unittest.TestCase):
    """Tests of LastDailyIpAddressOfUserTask.reducer()"""

    # Username is unicode here, not utf8
    username = u'test_user\u2603'
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
        input_1 = (self.timestamp, self.ip_address, self.course_id, self.username)
        input_2 = (self.earlier_timestamp, self.earlier_ip_address, self.course_id, self.username)

        mock_output_file = Mock()
        self.task.multi_output_reducer('random_date', [input_1, input_2], mock_output_file)
        self.assertEquals(len(mock_output_file.write.mock_calls), 2)
        expected_string = '\t'.join([self.timestamp, self.ip_address, self.username.encode('utf8'), self.course_id])
        self.assertEquals(mock_output_file.write.mock_calls[0], call(expected_string))
        self.assertEquals(mock_output_file.write.mock_calls[1], call('\n'))

    def test_output_path(self):
        output_path = self.task.output_path_for_key(self.DATE)
        expected_output_path = 's3://fake/warehouse/last_ip_of_user/dt={0}/last_ip_of_user_{0}'.format(self.DATE)
        self.assertEquals(output_path, expected_output_path)
        tasks = self.task.downstream_input_tasks()
        self.assertEquals(len(tasks), 1)
        self.assertEquals(tasks[0].url, expected_output_path)


class LastCountryOfUserMapperTestCase(MapperTestMixin, unittest.TestCase):
    """Tests of LastCountryOfUser.mapper()"""

    username = 'test_user'
    timestamp = '2013-12-17T15:38:32.805444'
    ip_address = FakeGeoLocation.ip_address_1
    course_id = 'DummyX/Course/ID'

    def setUp(self):
        self.task_class = LastCountryOfUser
        super(LastCountryOfUserMapperTestCase, self).setUp()

    def test_mapper(self):
        line = '\t'.join([self.timestamp, self.ip_address, self.username, self.course_id])
        self.assert_single_map_output(line, self.username, (self.timestamp, self.ip_address))

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


class LastCountryOfUserReducerTestCase(ReducerTestMixin, unittest.TestCase):
    """Tests of LastCountryOfUser.reducer()"""

    def setUp(self):
        self.task_class = LastCountryOfUser
        super(LastCountryOfUserReducerTestCase, self).setUp()

        self.username = "test_user"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.earlier_timestamp = "2013-12-15T15:38:32.805444"
        self.task.geoip = FakeGeoLocation()
        self.reduce_key = self.username

    def test_no_ip(self):
        self.assert_no_output([])

    def test_single_ip(self):
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_ip(self):
        inputs = [
            (self.earlier_timestamp, FakeGeoLocation.ip_address_1),
            (self.timestamp, FakeGeoLocation.ip_address_2),
        ]
        expected = (((FakeGeoLocation.country_name_2, FakeGeoLocation.country_code_2), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_multiple_ip_in_different_order(self):
        inputs = [
            (self.timestamp, FakeGeoLocation.ip_address_2),
            (self.earlier_timestamp, FakeGeoLocation.ip_address_1),
        ]
        expected = (((FakeGeoLocation.country_name_2, FakeGeoLocation.country_code_2), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_country_name_exception(self):
        self.task.geoip.country_name_by_addr = Mock(side_effect=Exception)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_country_code_exception(self):
        self.task.geoip.country_code_by_addr = Mock(side_effect=Exception)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_missing_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value=None)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_empty_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value="  ")
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_missing_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value=None)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_empty_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value="  ")
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_other_username(self):
        self.username = 'other_user'
        self.reduce_key = self.username
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1), self.username.encode('utf8')),)
        self._check_output_complete_tuple(inputs, expected)

    def test_unicode_username(self):
        self.username = 'I\xd4\x89\xef\xbd\x94\xc3\xa9\xef\xbd\x92\xd0\xbb\xc3\xa3\xef\xbd\x94\xc3\xac\xc3\xb2\xef\xbd\x8e\xc3\xa5\xc9\xad\xc3\xaf\xc8\xa5\xef\xbd\x81\xef\xbd\x94\xc3\xad\xdf\x80\xef\xbd\x8e'
        self.reduce_key = self.username.decode('utf8')
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1), self.username),)
        self._check_output_complete_tuple(inputs, expected)


class LastCountryOfUserPartitionTestCase(unittest.TestCase):
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

            ALTER TABLE last_country_of_user ADD IF NOT EXISTS PARTITION (dt='2014-01-01');
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
        self.assertEquals(required_task.output().path, 's3://fake/warehouse/last_country_of_user/dt=2014-01-01')


class QueryLastCountryPerCourseTaskTestCase(unittest.TestCase):
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
                date STRING,
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
            LEFT OUTER JOIN auth_user au on sce.user_id = au.id
            LEFT OUTER JOIN last_country_of_user uc on au.username = uc.username
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
        self.assertEquals(len(required_tasks[0]), 3)


class InsertToMysqlLastCountryPerCourseTaskTestCase(unittest.TestCase):
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
        print kwargs
        task = InsertToMysqlLastCountryPerCourseTask(**kwargs)
        required_tasks = task.requires()
        print required_tasks
        query_task = required_tasks['insert_source']
        self.assertTrue(query_task.overwrite)
