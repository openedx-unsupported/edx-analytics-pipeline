"""Verify that the calendar table is constructed correctly."""

from unittest import TestCase

from luigi import date_interval

from edx.analytics.tasks.insights.calendar_task import CalendarTask
from edx.analytics.tasks.util.tests.target import FakeTarget


class CalendarTaskTest(TestCase):
    """Verify that the calendar table is constructed correctly."""

    def test_single_day(self):
        table = self.generate_table(date_interval.Date.parse('2013-12-01'))
        self.assertEquals(len(table), 1)
        self.assertEquals(table[0], ['2013-12-01', '2013', '12', '1', '2013W48', '2013-11-25', '2013-12-02', '7'])

    def generate_table(self, interval):
        """Generate a calendar table containing every date in the provided interval."""
        output_target = FakeTarget()

        class TestCalendarTask(CalendarTask):
            output_root = None

            def output(self):
                return output_target

        c = TestCalendarTask(interval=interval)
        c.run()

        table = []
        for line in output_target.buffer.getvalue().splitlines():
            table.append(line.split('\t'))

        return table

    def test_mutliple_weeks(self):
        table = self.generate_table(date_interval.Custom.parse('2013-12-01-2013-12-03'))
        self.assertEquals(len(table), 2)
        self.assertEquals(table, [
            ['2013-12-01', '2013', '12', '1', '2013W48', '2013-11-25', '2013-12-02', '7'],
            ['2013-12-02', '2013', '12', '2', '2013W49', '2013-12-02', '2013-12-09', '1'],
        ])

    def test_leap_year(self):
        table = self.generate_table(date_interval.Custom.parse('2012-02-28-2012-03-02'))
        self.assertEquals(len(table), 3)
        self.assertEquals(table, [
            ['2012-02-28', '2012', '2', '28', '2012W09', '2012-02-27', '2012-03-05', '2'],
            ['2012-02-29', '2012', '2', '29', '2012W09', '2012-02-27', '2012-03-05', '3'],
            ['2012-03-01', '2012', '3', '1', '2012W09', '2012-02-27', '2012-03-05', '4'],
        ])
