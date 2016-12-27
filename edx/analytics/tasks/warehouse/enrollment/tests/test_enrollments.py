"""Tests for Enrollments-by-week report."""

import datetime
import textwrap
from StringIO import StringIO

import luigi
import luigi.hdfs
from mock import MagicMock
from numpy import isnan  # pylint: disable=no-name-in-module
import pandas

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget
from edx.analytics.tasks.reports.enrollments import EnrollmentsByWeek


class TestEnrollmentsByWeek(unittest.TestCase):
    """Tests for EnrollmentsByWeek report task."""

    def run_task(self, source, date, weeks, offset=None, statuses=None):
        """
        Run task with fake targets.

        Returns:
            the task output as a pandas dataframe.
        """

        parsed_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()

        # Make offsets None if it was not specified.
        task = EnrollmentsByWeek(name='fake_name',
                                 src=['fake_source'],
                                 offsets='fake_offsets' if offset else None,
                                 destination='fake_destination',
                                 date=parsed_date,
                                 weeks=weeks)

        # Mock the input and output targets

        def reformat(string):
            """Reformat string to make it like a TSV."""
            return textwrap.dedent(string).strip().replace(' ', '\t')

        input_targets = {
            'source': FakeTarget(value=reformat(source)),
        }

        # Mock offsets only if specified.
        if offset:
            input_targets.update({'offsets': FakeTarget(value=reformat(offset))})

        # Mock statuses only if specified.
        if statuses:
            input_targets.update({'statuses': FakeTarget(value=reformat(statuses))})

        task.input = MagicMock(return_value=input_targets)

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)

        # Run the task and parse the output into a pandas dataframe

        task.run()

        data = output_target.buffer.read()

        result = pandas.read_csv(StringIO(data),
                                 na_values=['-'],
                                 index_col='course_id')

        return result

    def test_parse_source(self):
        source = """
        course_1 2013-01-01 10
        course_1 2013-01-02 10
        course_1 2013-01-03 10
        course_1 2013-01-09 10
        course_1 2013-01-17 10
        course_2 2013-01-01 10
        course_3 2013-01-01 10
        """
        res = self.run_task(source, '2013-01-17', 3)

        self.assertEqual(set(['course_1', 'course_2', 'course_3']),
                         set(res.index))

        self.assertEqual(res.loc['course_1']['2013-01-03'], 30)
        self.assertEqual(res.loc['course_1']['2013-01-10'], 40)
        self.assertEqual(res.loc['course_1']['2013-01-17'], 50)
        self.assertEqual(res.loc['course_2']['2013-01-03'], 10)
        self.assertEqual(res.loc['course_3']['2013-01-03'], 10)

    def test_week_grouping(self):
        source = """
        course_1 2013-01-06 10
        course_1 2013-01-14 10
        """
        res = self.run_task(source, '2013-01-21', 4)
        weeks = set(['2012-12-31', '2013-01-07', '2013-01-14', '2013-01-21'])
        self.assertEqual(weeks | set(['org_id', 'status']),
                         set(str(w) for w in res.columns))

        course_1 = res.loc['course_1']
        self.assertTrue(isnan(course_1['2012-12-31']))  # no data
        self.assertEqual(course_1['2013-01-07'], 10)
        self.assertEqual(course_1['2013-01-14'], 20)
        self.assertTrue(isnan(course_1['2013-01-21']))  # no data

    def test_cumulative(self):
        source = """
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
        res = self.run_task(source, '2013-02-18', 2)

        course_1 = res.loc['course_1']
        self.assertEqual(course_1['2013-02-11'], 13)
        self.assertEqual(course_1['2013-02-18'], 21)

        course_2 = res.loc['course_2']
        self.assertEqual(course_2['2013-02-11'], 0)
        self.assertEqual(course_2['2013-02-18'], 3)

    def test_offsets(self):
        source = """
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

        offset = """
        course_2 2013-03-07 8
        course_3 2013-03-15 6
        course_4 2013-03-12 150000
        """
        res = self.run_task(source, '2013-03-28', 4, offset=offset)

        course_2 = res.loc['course_2']
        self.assertEqual(course_2['2013-03-07'], 9)
        self.assertEqual(course_2['2013-03-14'], 12)

        course_3 = res.loc['course_3']
        self.assertTrue(isnan(course_3['2013-03-07']))  # no data
        self.assertTrue(isnan(course_3['2013-03-14']))  # no data
        self.assertEqual(course_3['2013-03-21'], 9)

        course_4 = res.loc['course_4']
        self.assertTrue(isnan(course_4['2013-03-07']))  # no data
        self.assertEqual(course_4['2013-03-14'], 150000)
        self.assertEqual(course_4['2013-03-21'], 150000)

    def test_unicode(self):
        course_id = u'course_\u2603'

        source = u"""
        {course_id} 2013-04-01 1
        {course_id} 2013-04-02 1
        """.format(course_id=course_id)

        res = self.run_task(source.encode('utf-8'), '2013-04-02', 1)

        self.assertEqual(res.loc[course_id.encode('utf-8')]['2013-04-02'], 2)

    def test_task_urls(self):
        date = datetime.date(2013, 01, 20)

        task = EnrollmentsByWeek(name='fake_name',
                                 src=['s3://bucket/path/'],
                                 offsets='s3://bucket/file.txt',
                                 destination='file://path/file.txt',
                                 date=date)

        requires = task.requires()

        source = requires['source'].output()
        offsets = requires['offsets'].output()
        self.assertIsInstance(offsets, luigi.hdfs.HdfsTarget)
        self.assertEqual(offsets.format, luigi.hdfs.Plain)

        destination = task.output()
        self.assertIsInstance(destination, luigi.File)

    def test_statuses(self):
        source = """
        course_1 2013-03-01 1
        course_2 2013-03-07 1
        course_3 2013-03-15 1
        """

        statuses = """
        course_2 new
        course_3 past
        """
        res = self.run_task(source, '2013-03-28', 4, statuses=statuses)

        self.assertTrue(isnan(res.loc['course_1']['status']))
        self.assertEquals(res.loc['course_2']['status'], 'new')
        self.assertEquals(res.loc['course_3']['status'], 'past')

    def test_organization_mapping(self):
        source = """
        foo/course_1/run 2013-03-01 1
        bar/course_2/run 2013-03-07 1
        baz/course_3 2013-03-15 1
        course_4 2013-03-16 1
        """
        res = self.run_task(source, '2013-03-28', 4)

        self.assertEquals(res.loc['foo/course_1/run']['org_id'], 'foo')
        self.assertEquals(res.loc['bar/course_2/run']['org_id'], 'bar')
        self.assertTrue(isnan(res.loc['baz/course_3']['org_id']))
        self.assertTrue(isnan(res.loc['course_4']['org_id']))
