"""Tests for Total Users and Enrollment report."""

import datetime
import textwrap
from StringIO import StringIO
from mock import MagicMock
from numpy import isnan  # pylint: disable=no-name-in-module
import pandas

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget
from edx.analytics.tasks.reports.incremental_enrollments import WeeklyIncrementalUsersAndEnrollments
from edx.analytics.tasks.reports.incremental_enrollments import DailyRegistrationsEnrollmentsAndCourses


class TestWeeklyIncrementalUsersAndEnrollments(unittest.TestCase):
    """Tests for WeeklyIncrementalUsersAndEnrollments class."""

    @staticmethod
    def row_label(row_name):
        """Returns label value for reference row, given its internal row name."""
        return WeeklyIncrementalUsersAndEnrollments.ROW_LABELS[row_name]

    def run_task(self, registrations, enrollments, date, weeks, blacklist=None):
        """
        Run task with fake targets.

        Returns:
            the task output as a pandas dataframe.
        """

        parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

        task = WeeklyIncrementalUsersAndEnrollments(
            name="fake_name",
            destination='fake_destination',
            date=parsed_date,
            weeks=weeks,
            blacklist=blacklist
        )

        # Default missing inputs
        if registrations is None:
            registrations = """
                2013-01-01 10
                2013-01-10 20
                """
        if enrollments is None:
            enrollments = """
                course_1 2013-01-06 10
                course_1 2013-01-14 10
                """

        # Mock the input and output targets
        def reformat(string):
            """Reformat string to make it like a TSV."""
            return textwrap.dedent(string).strip().replace(' ', '\t')

        input_targets = {
            'enrollments': FakeTarget(value=reformat(enrollments)),
            'registrations': FakeTarget(value=reformat(registrations)),
        }

        # Mock blacklist only if specified.
        if blacklist:
            input_targets.update({'blacklist': FakeTarget(value=reformat(blacklist))})

        task.input = MagicMock(return_value=input_targets)

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)

        # Run the task and parse the output into a pandas dataframe

        task.run()

        data = output_target.buffer.read()
        result = pandas.read_csv(StringIO(data),
                                 na_values=['-'],
                                 index_col=self.row_label('header'))

        return result

    def test_parse_registrations(self):
        registrations = """
        2012-12-20 1
        2013-01-01 10
        2013-01-02 11
        2013-01-03 12
        2013-01-09 13
        2013-01-17 14
        """
        res = self.run_task(registrations, None, '2013-01-17', 3)
        self.assertEqual(set(['2013-01-03', '2013-01-10', '2013-01-17']),
                         set(res.columns))

        inc_registration = res.loc[self.row_label('registration_change')]
        self.assertEqual(inc_registration['2013-01-03'], 33)
        self.assertEqual(inc_registration['2013-01-10'], 13)
        self.assertEqual(inc_registration['2013-01-17'], 14)

    def test_parse_enrollments(self):
        enrollments = """
        course_0 2012-12-20 1
        course_1 2013-01-01 10
        course_1 2013-01-02 11
        course_1 2013-01-03 12
        course_1 2013-01-09 13
        course_1 2013-01-17 14
        course_2 2013-01-01 15
        course_3 2013-01-01 16
        """
        res = self.run_task(None, enrollments, '2013-01-17', 3)
        self.assertEqual(set(['2013-01-03', '2013-01-10', '2013-01-17']),
                         set(res.columns))

        inc_enrollment = res.loc[self.row_label('enrollment_change')]
        self.assertEqual(inc_enrollment['2013-01-03'], 64)
        self.assertEqual(inc_enrollment['2013-01-10'], 13)
        self.assertEqual(inc_enrollment['2013-01-17'], 14)

    def test_week_grouping(self):
        # A range of valid data that starts on a week boundary:
        registrations = """
        2013-01-01 11
        2013-01-10 22
        """
        # A range of valid data that ends on a week boundary:
        enrollments = """
        course_1 2013-01-06 13
        course_1 2013-01-14 14
        """
        res = self.run_task(registrations, enrollments, '2013-01-21', 4)
        weeks = set(['2012-12-31', '2013-01-07', '2013-01-14', '2013-01-21'])
        self.assertEqual(weeks, set(str(w) for w in res.columns))

        inc_registration = res.loc[self.row_label('registration_change')]
        self.assertTrue(isnan(inc_registration['2012-12-31']))  # no data
        self.assertEqual(inc_registration['2013-01-07'], 11)
        self.assertTrue(isnan(inc_registration['2013-01-14']))  # no data
        self.assertTrue(isnan(inc_registration['2013-01-21']))  # no data

        inc_enrollment = res.loc[self.row_label('enrollment_change')]
        self.assertTrue(isnan(inc_enrollment['2012-12-31']))  # no data
        self.assertTrue(isnan(inc_enrollment['2013-01-07']))  # no data
        self.assertEqual(inc_enrollment['2013-01-14'], 14)
        self.assertTrue(isnan(inc_enrollment['2013-01-21']))  # no data

    def test_less_than_week(self):
        registrations = """
        2013-01-01 11
        2013-01-05 22
        """
        enrollments = """
        course_1 2013-01-15 13
        course_1 2013-01-17 14
        """
        res = self.run_task(registrations, enrollments, '2013-01-21', 4)
        weeks = set(['2012-12-31', '2013-01-07', '2013-01-14', '2013-01-21'])
        self.assertEqual(weeks, set(str(w) for w in res.columns))

        inc_registration = res.loc[self.row_label('registration_change')]
        inc_enrollment = res.loc[self.row_label('enrollment_change')]
        for date in weeks:
            self.assertTrue(isnan(inc_registration[date]))
            self.assertTrue(isnan(inc_enrollment[date]))

    def test_non_overlapping_weeks(self):
        registrations = """
        2013-01-01 11
        2013-01-10 22
        """
        enrollments = """
        course_1 2013-01-15 13
        course_1 2013-01-21 14
        """
        res = self.run_task(registrations, enrollments, '2013-01-21', 4)
        weeks = set(['2012-12-31', '2013-01-07', '2013-01-14', '2013-01-21'])
        self.assertEqual(weeks, set(str(w) for w in res.columns))

        inc_registration = res.loc[self.row_label('registration_change')]
        self.assertTrue(isnan(inc_registration['2012-12-31']))  # no data
        self.assertEqual(inc_registration['2013-01-07'], 11)
        self.assertTrue(isnan(inc_registration['2013-01-14']))  # no data
        self.assertTrue(isnan(inc_registration['2013-01-21']))  # no data

        inc_enrollment = res.loc[self.row_label('enrollment_change')]
        self.assertTrue(isnan(inc_enrollment['2012-12-31']))  # no data
        self.assertTrue(isnan(inc_enrollment['2013-01-07']))  # no data
        self.assertTrue(isnan(inc_enrollment['2013-01-14']))  # no data
        self.assertEqual(inc_enrollment['2013-01-21'], 27)

    def test_incremental_registration(self):
        registrations = """
        2013-02-01 4
        2013-02-04 4
        2013-02-08 5
        2013-02-12 -2
        2013-02-14 3
        2013-02-15 -2
        2013-02-16 6
        2013-02-18 6
        """
        res = self.run_task(registrations, None, '2013-02-18', 2)
        weeks = set(['2013-02-11', '2013-02-18'])
        self.assertEqual(weeks, set(str(w) for w in res.columns))
        inc_registration = res.loc[self.row_label('registration_change')]
        self.assertEqual(inc_registration['2013-02-11'], 5)
        self.assertEqual(inc_registration['2013-02-18'], 11)

        # also test averages:
        avg_registration = res.loc[self.row_label('average_registration_change')]
        self.assertEqual(avg_registration['2013-02-11'], 5 / 7)
        self.assertEqual(avg_registration['2013-02-18'], 11 / 7)

    def test_incremental_enrollment(self):
        enrollments = """
        course_1 2013-02-01 4
        course_1 2013-02-04 4
        course_1 2013-02-08 5
        course_1 2013-02-12 -4
        course_1 2013-02-16 6
        course_1 2013-02-18 6
        course_2 2013-02-12 2
        course_2 2013-02-14 3
        course_2 2013-02-15 -2
        """
        res = self.run_task(None, enrollments, '2013-02-18', 2)
        weeks = set(['2013-02-11', '2013-02-18'])
        self.assertEqual(weeks, set(str(w) for w in res.columns))
        inc_enrollment = res.loc[self.row_label('enrollment_change')]
        self.assertEqual(inc_enrollment['2013-02-11'], 5)
        self.assertEqual(inc_enrollment['2013-02-18'], 11)

        # also test averages:
        avg_enrollment = res.loc[self.row_label('average_enrollment_change')]
        self.assertEqual(avg_enrollment['2013-02-11'], 5 / 7)
        self.assertEqual(avg_enrollment['2013-02-18'], 11 / 7)

    def test_output_row_order(self):
        res = self.run_task(None, None, '2013-02-18', 2)
        expected_rows = [
            self.row_label('registration_change'),
            self.row_label('average_registration_change'),
            self.row_label('enrollment_change'),
            self.row_label('average_enrollment_change'),
        ]
        self.assertEqual(res.index.tolist(), expected_rows)

    def test_unicode_course_id(self):
        course_id = u'course_\u2603'

        enrollments = u"""
        {course_id} 2013-03-20 1
        {course_id} 2013-04-01 2
        {course_id} 2013-04-02 3
        """.format(course_id=course_id)
        res = self.run_task(None, enrollments.encode('utf-8'), '2013-04-02', 2)

        self.assertEqual(res.loc[self.row_label('enrollment_change')]['2013-04-02'], 5)

    def test_blacklist(self):
        enrollments = """
        course_1 2013-01-02 1
        course_2 2013-01-02 2
        course_3 2013-01-02 4
        course_2 2013-01-09 1
        course_3 2013-01-15 2
        """
        blacklist = """
        course_1
        course_2
        """
        res = self.run_task(None, enrollments, '2013-01-15', 2, blacklist=blacklist)
        self.assertEqual(res.loc[self.row_label('enrollment_change')]['2013-01-08'], 4)
        self.assertEqual(res.loc[self.row_label('enrollment_change')]['2013-01-15'], 2)


class TestDailyRegistrationsEnrollmentsAndCourses(unittest.TestCase):
    """Tests for DailyRegistrationsEnrollmentsAndCourses class."""

    @staticmethod
    def row_label(row_name):
        """Returns label value for reference row, given its internal row name."""
        return DailyRegistrationsEnrollmentsAndCourses.ROW_LABELS[row_name]

    def setUp(self):
        self.enrollment_label = DailyRegistrationsEnrollmentsAndCourses.ROW_LABELS['enrollments']
        self.registrations_label = DailyRegistrationsEnrollmentsAndCourses.ROW_LABELS['registrations']

    def run_task(self, registrations, enrollments, date, days, blacklist=None):
        """
        Run task with fake targets.

        Returns:
            the task output as a pandas dataframe.
        """

        parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

        # Make offsets None if it was not specified.
        task = DailyRegistrationsEnrollmentsAndCourses(
            name="fake_name",
            destination='fake_destination',
            date=parsed_date,
            days=days,
            blacklist=blacklist
        )

        # Default missing inputs
        if registrations is None:
            registrations = """
                2013-01-01 10
                2013-01-10 20
                """

        if enrollments is None:
            enrollments = """
                course_1 2013-01-06 10
                course_1 2013-01-14 10
                """

        # Mock the input and output targets

        def reformat(string):
            # Reformat string to make it like a hadoop tsv
            return textwrap.dedent(string).strip().replace(' ', '\t')

        input_targets = {
            'enrollments': FakeTarget(value=reformat(enrollments)),
            'registrations': FakeTarget(value=reformat(registrations)),
        }

        # Mock blacklist only if specified.
        if blacklist:
            input_targets.update({'blacklist': FakeTarget(value=reformat(blacklist))})

        task.input = MagicMock(return_value=input_targets)

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)

        # Run the task and parse the output into a pandas dataframe

        task.run()

        data = output_target.buffer.read()
        result = pandas.read_csv(StringIO(data),
                                 na_values=['-'],
                                 index_col=self.row_label('header'))

        return result

    def test_incremental_registration(self):
        registrations = """
        2013-02-15 -2
        2013-02-16 6
        2013-02-18 6
        """

        res = self.run_task(registrations, None, '2013-02-19', 6)
        days = set(['2013-02-14', '2013-02-15', '2013-02-16', '2013-02-17', '2013-02-18', '2013-02-19'])
        self.assertEqual(days, set(str(col) for col in res.columns))

        inc_registration = res.loc[self.registrations_label]
        self.assertTrue(isnan(inc_registration['2013-02-14']))
        self.assertEqual(inc_registration['2013-02-15'], -2)
        self.assertEqual(inc_registration['2013-02-16'], 6)
        self.assertEqual(inc_registration['2013-02-17'], 0)
        self.assertEqual(inc_registration['2013-02-18'], 6)
        self.assertTrue(isnan(inc_registration['2013-02-19']))

    def test_incremental_enrollment(self):
        enrollments = """
        course_1 2013-02-01 4
        course_1 2013-02-18 6
        course_2 2013-02-17 3
        course_2 2013-02-18 -2
        """
        res = self.run_task(None, enrollments, '2013-02-19', 4)
        days = set(['2013-02-16', '2013-02-17', '2013-02-18', '2013-02-19'])
        self.assertEqual(days, set(str(d) for d in res.columns))

        inc_enrollment = res.loc[self.enrollment_label]
        self.assertEqual(inc_enrollment['2013-02-16'], 0)
        self.assertEqual(inc_enrollment['2013-02-17'], 3)
        self.assertEqual(inc_enrollment['2013-02-18'], 4)
        self.assertTrue(isnan(inc_enrollment['2013-02-19']))

    def test_output_row_order(self):
        res = self.run_task(None, None, '2013-02-18', 2)
        expected_rows = [
            self.registrations_label,
            self.enrollment_label,
        ]
        self.assertEqual(res.index.tolist(), expected_rows)

    def test_blacklist(self):
        enrollments = """
        course_1 2013-01-02 1
        course_2 2013-01-02 2
        course_3 2013-01-02 4
        course_2 2013-01-09 1
        course_3 2013-01-15 2
        """
        blacklist = """
        course_1
        course_2
        """
        res = self.run_task(None, enrollments, '2013-01-15', 20, blacklist=blacklist)
        self.assertEqual(res.loc[self.enrollment_label]['2013-01-02'], 4)
        self.assertEqual(res.loc[self.enrollment_label]['2013-01-10'], 0)
        self.assertEqual(res.loc[self.enrollment_label]['2013-01-15'], 2)
