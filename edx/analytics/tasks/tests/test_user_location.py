"""
Tests for user geolocation tasks.
"""
import datetime
import json
import tempfile
import os
import shutil
import textwrap

from mock import Mock, MagicMock, patch

import luigi.worker

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.user_location import LastCountryForEachUser
from edx.analytics.tasks.user_location import UsersPerCountry
from edx.analytics.tasks.user_location import UsersPerCountryReport
from edx.analytics.tasks.user_location import UsersPerCountryReportWorkflow
from edx.analytics.tasks.user_location import UNKNOWN_COUNTRY, UNKNOWN_CODE
from edx.analytics.tasks.tests.target import FakeTarget


class FakeGeoLocation(object):
    """Fake version of pygeoip.GeoIp() instance for use in testing."""

    ip_address_1 = "123.45.67.89"
    ip_address_2 = "98.76.54.123"
    country_name_1 = "COUNTRY NAME 1"
    country_code_1 = "COUNTRY CODE 1"
    country_name_2 = "COUNTRY NAME 2"
    country_code_2 = "COUNTRY CODE 2"

    def country_name_by_addr(self, ip_address):
        """Generates a country name if ip address is recognized."""
        country_name_map = {
            self.ip_address_1: self.country_name_1,
            self.ip_address_2: self.country_name_2,
        }
        return country_name_map.get(ip_address)

    def country_code_by_addr(self, ip_address):
        """Generates a country code if ip address is recognized."""
        country_code_map = {
            self.ip_address_1: self.country_code_1,
            self.ip_address_2: self.country_code_2,
        }
        return country_code_map.get(ip_address)


class BaseUserLocationEventTestCase(MapperTestMixin, unittest.TestCase):
    """Provides create-event functionality for testing user location tasks."""

    username = 'test_user'
    timestamp = "2013-12-17T15:38:32.805444"
    ip_address = FakeGeoLocation.ip_address_1

    def setUp(self):
        super(BaseUserLocationEventTestCase, self).setUp()

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


class LastCountryForEachUserMapperTestCase(BaseUserLocationEventTestCase):
    """Tests of LastCountryForEachUser.mapper()"""

    def setUp(self):
        self.task_class = LastCountryForEachUser

        super(LastCountryForEachUserMapperTestCase, self).setUp()

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


class LastCountryForEachUserReducerTestCase(ReducerTestMixin, unittest.TestCase):
    """Tests of LastCountryForEachUser.reducer()"""

    def setUp(self):
        self.task_class = LastCountryForEachUser
        super(LastCountryForEachUserReducerTestCase, self).setUp()

        print self.task

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


class UsersPerCountryTestCase(MapperTestMixin, ReducerTestMixin, unittest.TestCase):
    """Tests of UsersPerCountry."""

    def setUp(self):
        self.task_class = UsersPerCountry
        self.end_date = datetime.date(2014, 4, 1),
        super(UsersPerCountryTestCase, self).setUp()

    def _create_input_line(self, country, code, username):
        """Generates input matching what LastCountryForEachUser.reducer() would produce."""
        return "{country}\t{code}\t{username}".format(country=country, code=code, username=username)

    def test_mapper_on_normal(self):
        line = self._create_input_line("COUNTRY", "CODE", "USER")
        self.assert_single_map_output(line, ('COUNTRY', 'CODE'), 1)

    def test_mapper_with_empty_country(self):
        line = self._create_input_line("", "CODE", "USER")
        self.assert_no_map_output_for(line)

    def test_reducer(self):
        self.reduce_key = ("Country_1", "Code_1")
        values = [34, 29, 102]
        expected = ((self.reduce_key, sum(values), datetime.date(2014, 4, 1)),)
        self._check_output_complete_tuple(values, expected)


class UsersPerCountryReportTestCase(unittest.TestCase):
    """Tests of UsersPerCountryReport."""

    def run_task(self, counts):
        """
        Run task with fake targets.

        Returns:
            the task output as a string.
        """
        task = UsersPerCountryReport(counts='fake_counts', report='fake_report')

        def reformat(string):
            """Reformat string to make it like a TSV."""
            return textwrap.dedent(string).strip().replace(' ', '\t')

        task.input = MagicMock(return_value=FakeTarget(value=reformat(counts)))
        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)

        task.run()
        return output_target.buffer.read()

    def test_report(self):
        date = '2014-04-01'
        # Output counts in reverse order, to confirm that sorting works.
        counts = """
                Country_1 Code_1 34 {date}
                Country_2 Code_2 43 {date}
                """.format(date=date)
        output = self.run_task(counts)
        output_lines = output.split('\n')
        self.assertEquals(output_lines[0], UsersPerCountryReport.create_header(date))
        self.assertEquals(
            output_lines[1], UsersPerCountryReport.create_csv_entry(float(43) / 77, 43, "Country_2", "Code_2")
        )
        self.assertEquals(
            output_lines[2], UsersPerCountryReport.create_csv_entry(float(34) / 77, 34, "Country_1", "Code_1")
        )
        # Also confirm the formatting:
        for line in output_lines[1:2]:
            self.assertTrue(line.startswith('0.'))


class UsersPerCountryReportWorkflowTestCase(BaseUserLocationEventTestCase):
    """Tests of UsersPerCountryReportWorkflow."""

    def setUp(self):
        # Define a real output directory, so it can
        # be removed if existing.
        def cleanup(dirname):
            """Remove the temp directory only if it exists."""
            if os.path.exists(dirname):
                shutil.rmtree(dirname)

        self.temp_rootdir = tempfile.mkdtemp()
        self.addCleanup(cleanup, self.temp_rootdir)

    def test_workflow(self):
        # set up directories:
        src_path = os.path.join(self.temp_rootdir, "src")
        os.mkdir(src_path)
        counts_path = os.path.join(self.temp_rootdir, "counts")
        os.mkdir(counts_path)
        report_path = os.path.join(self.temp_rootdir, "report.csv")
        data_filepath = os.path.join(self.temp_rootdir, "geoloc.dat")
        with open(data_filepath, 'w') as data_file:
            data_file.write("Dummy geolocation data.")

        # create input:
        log_filepath = os.path.join(src_path, "tracking.log")
        with open(log_filepath, 'w') as log_file:
            log_file.write(self._create_event_log_line())
            log_file.write('\n')
            log_file.write(self._create_event_log_line(username="second_user", ip=FakeGeoLocation.ip_address_2))
            log_file.write('\n')

        end_date = '2014-04-01'
        task = UsersPerCountryReportWorkflow(
            mapreduce_engine='local',
            name='test',
            src=[src_path],
            end_date=datetime.datetime.strptime(end_date, '%Y-%m-%d').date(),
            geolocation_data=data_filepath,
            counts=counts_path,
            report=report_path,
        )
        worker = luigi.worker.Worker()
        worker.add(task)
        with patch('edx.analytics.tasks.user_location.pygeoip') as mock_pygeoip:
            mock_pygeoip.GeoIP = Mock(return_value=FakeGeoLocation())
            worker.run()
        worker.stop()

        output_lines = []
        with open(report_path) as report_file:
            output_lines = report_file.readlines()

        self.assertEquals(len(output_lines), 3)
        self.assertEquals(output_lines[0].strip('\n'), UsersPerCountryReport.create_header(end_date))
        expected = UsersPerCountryReport.create_csv_entry(
            0.5, 1, FakeGeoLocation.country_name_1, FakeGeoLocation.country_code_1
        )
        self.assertEquals(output_lines[1].strip('\n'), expected)
        expected = UsersPerCountryReport.create_csv_entry(
            0.5, 1, FakeGeoLocation.country_name_2, FakeGeoLocation.country_code_2
        )
        self.assertEquals(output_lines[2].strip('\n'), expected)
