"""
Test total events daily report
"""

import os
import shutil
import tempfile
import textwrap
from StringIO import StringIO
from unittest import TestCase

import pandas
from mock import MagicMock

from edx.analytics.tasks.monitor.total_events_report import TotalEventsReport
from edx.analytics.tasks.util.tests.target import FakeTarget


class TestTotalEventsReport(TestCase):
    """Tests for TotalEventsReport report task."""

    def setUp(self):
        self.temp_rootdir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.temp_rootdir, "input")
        os.mkdir(self.input_dir)
        self.input_file = os.path.join(self.input_dir, "my_file")
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    def test_create_csv_header(self):
        header = TotalEventsReport.create_header()
        self.assertEqual(header, "date,event_count")

    def test_create_csv_entry(self):
        entry = TotalEventsReport.create_csv_entry('2014-01-02', '5000')
        self.assertEqual(entry, '2014-01-02,5000')

    def test_vanilla(self):
        source = '2014-10-03 300'
        out = self.run_task(source)
        self.assertEqual(out.loc[1]['count'], '300')
        self.assertEqual(out.loc[1]['date'], '2014-10-03')

    def test_multilines_sorted(self):
        source = """
        2014-10-03 300
        2014-10-02 15
        2015-11-01 40
        """
        out = self.run_task(source)
        # Info for the 2015 date should appear first
        self.assertEqual(out.loc[1]['count'], '40')
        self.assertEqual(out.loc[1]['date'], '2015-11-01')
        self.assertEqual(out.loc[2]['count'], '300')
        self.assertEqual(out.loc[2]['date'], '2014-10-03')

    def test_empty_source(self):
        """Resulting dataframe contains only the csv headers"""
        source = """
        """
        out = self.run_task(source)
        # Assert dataframe shape: 1 row, 2 columns (i.e. the headers)
        self.assertEqual(out.shape, (1, 2))
        self.assertEqual(out.loc[0]['count'], 'event_count')
        self.assertEqual(out.loc[0]['date'], 'date')

    def run_task(self, source):
        """Helper utility for running task under test"""

        def reformat(string):
            """Reformat string to make it like a TSV."""
            return textwrap.dedent(string).strip().replace(' ', '\t')

        with open(self.input_file, 'w') as fle:
            fle.write(reformat(source))

        task = TotalEventsReport(counts=self.input_file,
                                 report='fake_report')

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)
        task.run()
        data = output_target.buffer.read()

        result = pandas.read_csv(StringIO(data),
                                 na_values=['-'],
                                 index_col=False,
                                 header=None,
                                 names=['date', 'count'])

        return result
