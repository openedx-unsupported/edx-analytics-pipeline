"""
Tests for geolocation-per-course tasks.
"""
import json
import textwrap
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin

from mock import Mock, patch

from luigi.date_interval import Year

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.location_per_course import (
    ImportLastCountryOfUserToHiveTask,
    LastCountryOfUser, QueryLastCountryPerCourseTask,
    QueryLastCountryPerCourseWorkflow,
    InsertToMysqlCourseEnrollByCountryWorkflow,
)
from edx.analytics.tasks.util.geolocation import UNKNOWN_COUNTRY, UNKNOWN_CODE
from edx.analytics.tasks.util.tests.test_geolocation import FakeGeoLocation


class LastCountryOfUserMapperTestCase(MapperTestMixin, unittest.TestCase):
    """Tests of LastCountryOfUser.mapper()"""

    username = 'test_user'
    timestamp = "2013-12-17T15:38:32.805444"
    ip_address = FakeGeoLocation.ip_address_1

    def setUp(self):
        self.task_class = LastCountryOfUser
        super(LastCountryOfUserMapperTestCase, self).setUp()

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
        self.assert_single_map_output(line, self.username, (self.timestamp, self.ip_address))

    def test_username_with_newline(self):
        line = self._create_event_log_line(username="baduser\n")
        self.assert_single_map_output(line, "baduser", (self.timestamp, self.ip_address))


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
        expected = (((UNKNOWN_COUNTRY, UNKNOWN_CODE), self.username),)
        self._check_output_complete_tuple(inputs, expected)

    def test_country_code_exception(self):
        self.task.geoip.country_code_by_addr = Mock(side_effect=Exception)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, UNKNOWN_CODE), self.username),)
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
        self.username = 'I\xd4\x89\xef\xbd\x94\xc3\xa9\xef\xbd\x92\xd0\xbb\xc3\xa3\xef\xbd\x94\xc3\xac\xc3\xb2\xef\xbd\x8e\xc3\xa5\xc9\xad\xc3\xaf\xc8\xa5\xef\xbd\x81\xef\xbd\x94\xc3\xad\xdf\x80\xef\xbd\x8e'.decode('utf8')
        self.reduce_key = self.username
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1), self.username.encode('utf8')),)
        self._check_output_complete_tuple(inputs, expected)


class ImportLastCountryOfUserToHiveTestCase(unittest.TestCase):
    """Tests to validate ImportLastCountryOfUserToHiveTask."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating ImportLastCountryOfUserToHiveTask."""
        return {
            'interval': Year.parse('2013'),
        }

    def test_query_with_date_interval(self):
        task = ImportLastCountryOfUserToHiveTask(**self._get_kwargs())
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS last_country_of_user;
            CREATE EXTERNAL TABLE last_country_of_user (
                country_name STRING,country_code STRING,username STRING
            )
            PARTITIONED BY (dt STRING)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
            LOCATION 's3://fake/warehouse/last_country_of_user';
            ALTER TABLE last_country_of_user ADD PARTITION (dt = '2014-01-01');
            """
        )
        self.assertEquals(query, expected_query)

    def test_overwrite(self):
        kwargs = self._get_kwargs()
        kwargs['overwrite'] = True
        task = ImportLastCountryOfUserToHiveTask(**kwargs)
        self.assertFalse(task.complete())

    def test_no_overwrite(self):
        task = ImportLastCountryOfUserToHiveTask(**self._get_kwargs())
        with patch('edx.analytics.tasks.database_imports.HivePartitionTarget') as mock_target:
            output = mock_target()
            # Make MagicMock act more like a regular mock, so that flatten() does the right thing.
            del output.__iter__
            del output.__getitem__
            output.exists = Mock(return_value=False)
            self.assertFalse(task.complete())
            self.assertTrue(output.exists.called)
            output.exists = Mock(return_value=True)
            self.assertTrue(task.complete())
            self.assertTrue(output.exists.called)

    def test_requires(self):
        task = ImportLastCountryOfUserToHiveTask(**self._get_kwargs())
        required_task = task.requires()
        self.assertEquals(required_task.output().path, 's3://fake/warehouse/last_country_of_user/dt=2014-01-01')


class QueryLastCountryPerCourseTaskTestCase(unittest.TestCase):
    """Tests to validate QueryLastCountryPerCourseTask."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating QueryLastCountryPerCourseTask."""
        return {
            'course_country_output': 's3://output/path',
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
            LOCATION 's3://output/path';

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
        self.assertEquals(task.output().path, 's3://output/path')

    def test_requires(self):
        task = QueryLastCountryPerCourseTask(**self._get_kwargs())
        required_tasks = list(task.requires())
        self.assertEquals(len(required_tasks), 1)
        self.assertEquals(len(required_tasks[0]), 3)


class QueryLastCountryPerCourseWorkflowTestCase(unittest.TestCase):
    """Tests to validate QueryLastCountryPerCourseWorkflow."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating QueryLastCountryPerCourseWorkflow."""
        return {
            'interval': Year.parse('2013'),
            'course_country_output': 's3://output/course_country/path',
        }

    def test_output(self):
        task = QueryLastCountryPerCourseWorkflow(**self._get_kwargs())
        self.assertEquals(task.output().path, 's3://output/course_country/path')

    def test_requires(self):
        task = QueryLastCountryPerCourseWorkflow(**self._get_kwargs())
        required_tasks = list(task.requires())
        self.assertEquals(len(required_tasks), 1)
        self.assertEquals(len(required_tasks[0]), 4)


class InsertToMysqlCourseEnrollByCountryWorkflowTestCase(unittest.TestCase):
    """Tests to validate InsertToMysqlCourseEnrollByCountryWorkflow."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating InsertToMysqlCourseEnrollByCountryWorkflow."""
        return {
            'interval': Year.parse('2013'),
            'course_country_output': 's3://output/course_country/path',
            'credentials': 's3://config/credentials/output-database.json',
        }

    def test_requires(self):
        task = InsertToMysqlCourseEnrollByCountryWorkflow(**self._get_kwargs())
        required_tasks = task.requires()
        self.assertEquals(len(required_tasks), 2)
        self.assertEquals(required_tasks['credentials'].output().path, 's3://config/credentials/output-database.json')
        self.assertEquals(required_tasks['insert_source'].output().path, 's3://output/course_country/path')

    def test_requires_with_overwrite(self):
        kwargs = self._get_kwargs()
        kwargs['overwrite'] = True
        print kwargs
        task = InsertToMysqlCourseEnrollByCountryWorkflow(**kwargs)
        required_tasks = task.requires()
        print required_tasks
        query_task = required_tasks['insert_source']
        self.assertTrue(query_task.overwrite)
