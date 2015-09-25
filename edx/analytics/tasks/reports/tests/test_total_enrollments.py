"""Tests for Total Users and Enrollment report."""

import datetime
import textwrap
from StringIO import StringIO

import luigi
import luigi.hdfs
from mock import MagicMock
from numpy import isnan
import pandas

from edx.analytics.tasks.user_registrations import UserRegistrationsPerDay
from edx.analytics.tasks.reports.total_enrollments import WeeklyAllUsersAndEnrollments
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget


class TestWeeklyAllUsersAndEnrollments(unittest.TestCase):
    """Tests for WeeklyAllUsersAndEnrollments class."""

    @staticmethod
    def row_label(row_name):
        """Returns label value for reference row, given its internal row name."""
        return WeeklyAllUsersAndEnrollments.ROW_LABELS[row_name]

    def setUp(self):
        self.enrollment_label = WeeklyAllUsersAndEnrollments.ROW_LABELS['enrollments']
        self.registrations_label = WeeklyAllUsersAndEnrollments.ROW_LABELS['registrations']

    def run_task(self, registrations, enrollments, date, weeks, offset=None, history=None, blacklist=None):
        """
        Run task with fake targets.

        Returns:
            the task output as a pandas dataframe.
        """

        parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

        # Make offsets None if it was not specified.
        task = WeeklyAllUsersAndEnrollments(
            name='fake_name',
            n_reduce_tasks="fake_n_reduce_tasks",
            offsets='fake_offsets' if offset else None,
            history='fake_history' if history else None,
            destination='fake_destination',
            date=parsed_date,
            weeks=weeks,
            credentials=None,
            blacklist=blacklist
        )

        # Mock the input and output targets

        def reformat(string):
            """Reformat string to make it like a TSV."""
            return textwrap.dedent(string).strip().replace(' ', '\t')

        if enrollments is None:
            enrollments = """
                course_1 2013-03-01 1
                course_1 2013-03-30 2
                course_2 2013-03-07 1
                course_2 2013-03-08 1
                course_2 2013-03-10 1
                course_2 2013-03-13 1
                course_3 2013-03-15 1
                course_3 2013-03-18 1
                course_3 2013-03-19 1
                """

        input_targets = {
            'enrollments': FakeTarget(value=reformat(enrollments)),
            'registrations': FakeTarget(value=reformat(registrations))
        }

        # Mock offsets only if specified.
        if offset:
            input_targets.update({'offsets': FakeTarget(value=reformat(offset))})

        # Mock history only if specified.
        if history:
            input_targets.update({'history': FakeTarget(value=reformat(history))})

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

    def test_parsing(self):
        enrollments = """
        course_1 2013-01-01 10
        course_1 2013-01-02 10
        course_1 2013-01-03 10
        course_1 2013-01-09 10
        course_1 2013-01-17 10
        course_2 2013-01-01 10
        course_3 2013-01-01 10
        """
        registrations = """
        2013-01-01 30
        2013-01-08 10
        2013-01-17 10
        """
        res = self.run_task(registrations, enrollments, '2013-01-17', 3)
        self.assertEqual(set(['2013-01-03', '2013-01-10', '2013-01-17']),
                         set(res.columns))

        total_enrollment = res.loc[self.enrollment_label]
        self.assertEqual(total_enrollment['2013-01-03'], 50)
        self.assertEqual(total_enrollment['2013-01-10'], 60)
        self.assertEqual(total_enrollment['2013-01-17'], 70)

        reg_row = res.loc[self.registrations_label]
        self.assertEqual(reg_row['2013-01-03'], 30)
        self.assertEqual(reg_row['2013-01-10'], 40)
        self.assertEqual(reg_row['2013-01-17'], 50)

    def test_week_grouping(self):
        enrollments = """
        course_1 2013-01-06 10
        course_1 2013-01-14 10
        """
        registrations = """
        2013-01-06 10
        2013-01-14 10
        """
        res = self.run_task(registrations, enrollments, '2013-01-21', 4)
        weeks = set(['2012-12-31', '2013-01-07', '2013-01-14', '2013-01-21'])
        self.assertEqual(weeks, set(str(w) for w in res.columns))
        total_enrollment = res.loc[self.enrollment_label]
        self.assertTrue(isnan(total_enrollment['2012-12-31']))  # no data
        self.assertEqual(total_enrollment['2013-01-07'], 10)
        self.assertEqual(total_enrollment['2013-01-14'], 20)
        self.assertTrue(isnan(total_enrollment['2013-01-21']))  # no data

        reg_row = res.loc[self.registrations_label]
        self.assertEqual(reg_row['2013-01-07'], 10)
        self.assertEqual(reg_row['2013-01-14'], 20)

    def test_cumulative(self):
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
        res = self.run_task('', enrollments, '2013-02-18', 2)
        total_enrollment = res.loc[self.enrollment_label]
        self.assertEqual(total_enrollment['2013-02-11'], 13)
        self.assertEqual(total_enrollment['2013-02-18'], 24)

    def test_offsets(self):
        offset = """
        course_2 2013-03-07 8
        course_3 2013-03-15 6
        """
        res = self.run_task('', None, '2013-03-28', 6, offset=offset)
        total_enrollment = res.loc[self.enrollment_label]
        self.assertTrue(isnan(total_enrollment['2013-02-21']))  # no data
        self.assertTrue(isnan(total_enrollment['2013-02-28']))  # no data
        self.assertEqual(total_enrollment['2013-03-07'], 10)
        self.assertEqual(total_enrollment['2013-03-14'], 13)
        self.assertEqual(total_enrollment['2013-03-21'], 22)
        self.assertEqual(total_enrollment['2013-03-28'], 22)

    def test_non_overlapping_history(self):
        offset = """
        course_2 2013-03-07 8
        course_3 2013-03-15 6
        """
        # Choose history so that it ends right before
        # source data begins (on 3/1).
        history = """
        2013-02-21 4
        2013-02-28 10
        """

        res = self.run_task('', None, '2013-03-28', 6, offset=offset, history=history)
        total_enrollment = res.loc[self.enrollment_label]
        self.assertEqual(total_enrollment['2013-02-21'], 4)
        self.assertEqual(total_enrollment['2013-02-28'], 10)
        self.assertEqual(total_enrollment['2013-03-07'], 10)
        self.assertEqual(total_enrollment['2013-03-14'], 13)
        self.assertEqual(total_enrollment['2013-03-21'], 22)
        self.assertEqual(total_enrollment['2013-03-28'], 22)

    def test_overlapping_history(self):
        offset = """
        course_2 2013-03-07 8
        course_3 2013-03-15 6
        """
        # Choose history so that it overlaps
        # with when source data begins (on 3/1).
        history = """
        2013-02-18 4
        2013-03-21 22
        """
        res = self.run_task('', None, '2013-03-28', 6, offset=offset, history=history)
        total_enrollment = res.loc[self.enrollment_label]
        print total_enrollment
        self.assertEqual(total_enrollment['2013-02-21'], 5)
        self.assertEqual(total_enrollment['2013-02-28'], 9)
        self.assertEqual(total_enrollment['2013-03-07'], 10)
        self.assertEqual(total_enrollment['2013-03-14'], 13)
        self.assertEqual(total_enrollment['2013-03-21'], 22)
        self.assertEqual(total_enrollment['2013-03-28'], 22)

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
        res = self.run_task('', enrollments, '2013-01-15', 2, blacklist=blacklist)
        self.assertEqual(res.loc[self.enrollment_label]['2013-01-08'], 4)
        self.assertEqual(res.loc[self.enrollment_label]['2013-01-15'], 6)

    def test_blacklist_course_not_in_enrollments(self):
        enrollments = """
        course_1 2013-01-02 1
        course_2 2013-01-02 2
        course_3 2013-01-02 4
        course_2 2013-01-09 1
        course_3 2013-01-15 2
        """
        blacklist = """
        course_4
        course_1
        course_2
        """
        res = self.run_task('', enrollments, '2013-01-15', 2, blacklist=blacklist)
        self.assertEqual(res.loc[self.enrollment_label]['2013-01-08'], 4)
        self.assertEqual(res.loc[self.enrollment_label]['2013-01-15'], 6)

    def test_unicode(self):
        course_id = u'course_\u2603'

        enrollments = u"""
        {course_id} 2013-04-01 1
        {course_id} 2013-04-02 1
        """.format(course_id=course_id)

        res = self.run_task('', enrollments.encode('utf-8'), '2013-04-02', 1)

        self.assertEqual(res.loc[self.enrollment_label]['2013-04-02'], 2)

    def test_task_configuration(self):
        date = datetime.date(2013, 01, 20)

        task = WeeklyAllUsersAndEnrollments(
            name='fake_name',
            n_reduce_tasks='fake_n_reduce_tasks',
            offsets='s3://bucket/file.txt',
            destination='s3://path/',
            history='file://path/history/file.gz',
            date=date,
            credentials='s3://bucket/cred.json'
        )

        requires = task.requires()

        enrollments = requires['enrollments'].output()
        self.assertIsInstance(enrollments, luigi.hdfs.HdfsTarget)
        self.assertEqual(enrollments.format, luigi.hdfs.PlainDir)

        offsets = requires['offsets'].output()
        self.assertIsInstance(offsets, luigi.hdfs.HdfsTarget)
        self.assertEqual(offsets.format, luigi.hdfs.Plain)

        history = requires['history'].output()
        self.assertIsInstance(history, luigi.File)

        registrations = requires['registrations'].output()
        self.assertIsInstance(requires['registrations'], UserRegistrationsPerDay)
        self.assertEqual(registrations.path, 's3://path/user_registrations_1900-01-01-2013-01-21.tsv')
        self.assertIsInstance(registrations, luigi.hdfs.HdfsTarget)
        self.assertEqual(registrations.format, luigi.hdfs.Plain)

        destination = task.output()

        self.assertEqual(destination.path, 's3://path/total_users_and_enrollments_2012-01-22-2013-01-20.csv')
        self.assertIsInstance(offsets, luigi.hdfs.HdfsTarget)
        self.assertEqual(offsets.format, luigi.hdfs.Plain)
