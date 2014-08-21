"""
Tests for geolocation-per-course tasks.
"""
import json
import textwrap

from mock import Mock, patch

from luigi.date_interval import Year

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.location_per_course import ImportLastCountryOfUserToHiveTask
from edx.analytics.tasks.location_per_course import LastCountryOfUser, QueryLastCountryPerCourseTask
from edx.analytics.tasks.location_per_course import QueryLastCountryPerCourseWorkflow
from edx.analytics.tasks.location_per_course import InsertToMysqlCourseEnrollByCountryWorkflow
from edx.analytics.tasks.user_location import UNKNOWN_COUNTRY, UNKNOWN_CODE
from edx.analytics.tasks.tests.test_user_location import FakeGeoLocation, BaseUserLocationEventTestCase


class LastCountryOfUserMapperTestCase(BaseUserLocationEventTestCase):
    """Tests of LastCountryOfUser.mapper()"""

    def setUp(self):
        self.task = LastCountryOfUser(
            mapreduce_engine='local',
            interval=Year.parse('2013'),
            output_root='s3://fake/warehouse/last_country_of_user'
        )
        self.task.init_local()

    def assert_no_output_for(self, line):
        """Assert that an input line generates no output."""
        self.assertEquals(tuple(self.task.mapper(line)), tuple())

    def test_non_enrollment_event(self):
        line = 'this is garbage'
        self.assert_no_output_for(line)

    def test_bad_datetime(self):
        line = self._create_event_log_line(time='this is a bogus time')
        self.assert_no_output_for(line)

    def test_after_end_date(self):
        line = self._create_event_log_line(time="2015-12-17T15:38:32.805444")
        self.assert_no_output_for(line)

    def test_missing_username(self):
        event_dict = self._create_event_dict()
        del event_dict['username']
        line = json.dumps(event_dict)
        self.assert_no_output_for(line)

    def test_missing_ip_address(self):
        event_dict = self._create_event_dict()
        del event_dict['ip']
        line = json.dumps(event_dict)
        self.assert_no_output_for(line)

    def test_good_event(self):
        line = self._create_event_log_line()
        event = tuple(self.task.mapper(line))
        expected = ((self.username, (self.timestamp, self.ip_address)),)
        self.assertEquals(event, expected)

    def test_username_with_newline(self):
        line = self._create_event_log_line(username="baduser\n")
        event = tuple(self.task.mapper(line))
        expected = (("baduser", (self.timestamp, self.ip_address)),)
        self.assertEquals(event, expected)


class LastCountryOfUserReducerTestCase(unittest.TestCase):
    """Tests of LastCountryOfUser.reducer()"""

    def setUp(self):
        self.username = "test_user"
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.earlier_timestamp = "2013-12-15T15:38:32.805444"
        self.task = LastCountryOfUser(
            mapreduce_engine='local',
            interval=Year.parse('2014'),
            output_root='s3://fake/warehouse/last_country_of_user'
        )
        self.task.geoip = FakeGeoLocation()

    def _get_reducer_output(self, values):
        """Run reducer with provided values hardcoded key."""
        return tuple(self.task.reducer(self.username, values))

    def _check_output(self, inputs, expected):
        """Compare generated with expected output."""
        self.assertEquals(self._get_reducer_output(inputs), expected)

    def test_no_ip(self):
        self._check_output([], tuple())

    def test_single_ip(self):
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1), self.username),)
        self._check_output(inputs, expected)

    def test_multiple_ip(self):
        inputs = [
            (self.earlier_timestamp, FakeGeoLocation.ip_address_1),
            (self.timestamp, FakeGeoLocation.ip_address_2),
        ]
        expected = (((FakeGeoLocation.country_name_2, FakeGeoLocation.country_code_2), self.username),)
        self._check_output(inputs, expected)

    def test_multiple_ip_in_different_order(self):
        inputs = [
            (self.timestamp, FakeGeoLocation.ip_address_2),
            (self.earlier_timestamp, FakeGeoLocation.ip_address_1),
        ]
        expected = (((FakeGeoLocation.country_name_2, FakeGeoLocation.country_code_2), self.username),)
        self._check_output(inputs, expected)

    def test_country_name_exception(self):
        self.task.geoip.country_name_by_addr = Mock(side_effect=Exception)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, UNKNOWN_CODE), self.username),)
        self._check_output(inputs, expected)

    def test_country_code_exception(self):
        self.task.geoip.country_code_by_addr = Mock(side_effect=Exception)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, UNKNOWN_CODE), self.username),)
        self._check_output(inputs, expected)

    def test_missing_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value=None)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.username),)
        self._check_output(inputs, expected)

    def test_empty_country_name(self):
        self.task.geoip.country_name_by_addr = Mock(return_value="  ")
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((UNKNOWN_COUNTRY, FakeGeoLocation.country_code_1), self.username),)
        self._check_output(inputs, expected)

    def test_missing_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value=None)
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.username),)
        self._check_output(inputs, expected)

    def test_empty_country_code(self):
        self.task.geoip.country_code_by_addr = Mock(return_value="  ")
        inputs = [(self.timestamp, FakeGeoLocation.ip_address_1)]
        expected = (((FakeGeoLocation.country_name_1, UNKNOWN_CODE), self.username),)
        self._check_output(inputs, expected)


class ImportLastCountryOfUserToHiveTestCase(unittest.TestCase):
    """Tests to validate ImportLastCountryOfUserToHiveTask."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating ImportLastCountryOfUserToHiveTask."""
        return {
            'interval': Year.parse('2013')
        }

    def test_query_with_date_interval(self):
        task = ImportLastCountryOfUserToHiveTask(**self._get_kwargs())
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;

            CREATE EXTERNAL TABLE last_country_of_user (
                country_name STRING,country_code STRING,username STRING
            )
            PARTITIONED BY (dt STRING)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
            LOCATION 's3://fake/warehouse/last_country_of_user/';
            ALTER TABLE last_country_of_user ADD PARTITION (dt='2014-01-01');
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
        with patch('edx.analytics.tasks.util.hive.HivePartitionTarget') as mock_target:
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
            'overwrite': True,
            'import_date': '2014-08-23'
        }

    def test_query(self):
        task = QueryLastCountryPerCourseTask(**self._get_kwargs())
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS course_enrollment_location_current;
            CREATE EXTERNAL TABLE course_enrollment_location_current (
                date STRING,course_id STRING,country_code STRING,count INT
            )
            PARTITIONED BY (dt STRING)
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
            LOCATION 's3://fake/warehouse/course_enrollment_location_current/';
            ALTER TABLE course_enrollment_location_current ADD PARTITION (dt='2014-08-23');

            INSERT INTO TABLE course_enrollment_location_current
            PARTITION (dt='2014-08-23')
            SELECT
                sce.dt,
                sce.course_id,
                uc.country_code,
                count(sce.user_id)
            FROM student_courseenrollment sce
            LEFT OUTER JOIN auth_user au on sce.user_id = au.id
            LEFT OUTER JOIN last_country_of_user uc on au.username = uc.username
            WHERE sce.is_active > 0
            GROUP BY sce.dt, sce.course_id, uc.country_code;
            """
        )
        self.assertEquals(query, expected_query)

    def test_output(self):
        task = QueryLastCountryPerCourseTask(**self._get_kwargs())
        self.assertEquals(task.output().path, 's3://fake/warehouse/course_enrollment_location_current/dt=2014-08-23')

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
            'import_date': '2014-08-23'
        }

    def test_output(self):
        task = QueryLastCountryPerCourseWorkflow(**self._get_kwargs())
        self.assertEquals(task.output().path, 's3://fake/warehouse/course_enrollment_location_current/dt=2014-08-23')

    def test_requires(self):
        task = QueryLastCountryPerCourseWorkflow(**self._get_kwargs())
        required_tasks = list(task.requires())
        self.assertEquals(len(required_tasks), 1)
        self.assertEquals(len(required_tasks[0]), 3)


class InsertToMysqlCourseEnrollByCountryWorkflowTestCase(unittest.TestCase):
    """Tests to validate InsertToMysqlCourseEnrollByCountryWorkflow."""

    def _get_kwargs(self):
        """Provides minimum args for instantiating InsertToMysqlCourseEnrollByCountryWorkflow."""
        return {
            'interval': Year.parse('2013'),
            'credentials': 's3://config/credentials/output-database.json',
            'import_date': '2014-08-23'
        }

    def test_requires(self):
        task = InsertToMysqlCourseEnrollByCountryWorkflow(**self._get_kwargs())
        required_tasks = task.requires()
        self.assertEquals(len(required_tasks), 2)
        self.assertEquals(required_tasks['credentials'].output().path, 's3://config/credentials/output-database.json')
        self.assertEquals(required_tasks['insert_source'].output().path, 's3://fake/warehouse/course_enrollment_location_current/dt=2014-08-23')

    def test_requires_with_overwrite(self):
        kwargs = self._get_kwargs()
        kwargs['overwrite'] = True
        print kwargs
        task = InsertToMysqlCourseEnrollByCountryWorkflow(**kwargs)
        required_tasks = task.requires()
        print required_tasks
        query_task = required_tasks['insert_source']
        self.assertTrue(query_task.overwrite)
