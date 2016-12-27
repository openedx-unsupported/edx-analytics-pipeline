"""
Ensure the user registrations task is setup properly.
"""
from __future__ import absolute_import

from luigi import date_interval
from mock import sentinel

from edx.analytics.tasks.user_registrations import UserRegistrationsPerDay
from edx.analytics.tasks.tests import unittest


class UserRegistrationsPerDayTestCase(unittest.TestCase):  # pylint: disable=missing-docstring

    def test_day(self):
        task = self.create_task(date_interval.Date.parse('2014-01-01'))
        self.assertEquals(task.query_parameters, ('2014-01-01 00:00:00', '2014-01-02 00:00:00'))

    def create_task(self, interval):
        """
        Args:
            interval (luigi.date_interval.DateInterval): The interval of dates to gather data for.

        Returns:
            A dummy task that uses the provided `interval` as it's date_interval.
        """
        return UserRegistrationsPerDay(
            credentials=sentinel.ignored,
            destination=sentinel.ignored,
            date_interval=interval
        )

    def test_week(self):
        task = self.create_task(date_interval.Week.parse('2014-W01'))
        self.assertEquals(task.query_parameters, ('2013-12-30 00:00:00', '2014-01-06 00:00:00'))

    def test_month(self):
        task = self.create_task(date_interval.Month.parse('2014-01'))
        self.assertEquals(task.query_parameters, ('2014-01-01 00:00:00', '2014-02-01 00:00:00'))

    def test_year(self):
        task = self.create_task(date_interval.Year.parse('2014'))
        self.assertEquals(task.query_parameters, ('2014-01-01 00:00:00', '2015-01-01 00:00:00'))

    def test_static_parameters(self):
        task = self.create_task(date_interval.Year.parse('2014'))
        self.assertEquals(task.query, "SELECT DATE(date_joined), COUNT(1) FROM `auth_user`"
                                      " WHERE `date_joined` >= %s AND `date_joined` < %s GROUP BY DATE(date_joined)"
                                      " ORDER BY 1 ASC")
        self.assertEquals(task.filename, 'user_registrations_2014.tsv')
