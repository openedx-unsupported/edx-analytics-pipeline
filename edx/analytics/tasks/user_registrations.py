"""
Determine the number of users that registered accounts each day.
"""
from __future__ import absolute_import

import datetime

import luigi

from edx.analytics.tasks.mysql_dump import MysqlSelectTask
from edx.analytics.tasks.mysql_dump import mysql_datetime


class UserRegistrationsPerDay(MysqlSelectTask):
    """
    Determine the number of users that registered accounts each day.

    """

    date_interval = luigi.DateIntervalParameter(
        description='The range of dates to gather data for.',
    )

    @property
    def query(self):
        return ("SELECT DATE(date_joined), COUNT(1) FROM `auth_user`"
                " WHERE `date_joined` >= %s AND `date_joined` < %s GROUP BY DATE(date_joined) ORDER BY 1 ASC")

    @property
    def query_parameters(self):
        dates = self.date_interval.dates()  # pylint: disable=no-member
        start_date = dates[0]
        # Note that we could probably use the end date at 23:59:59, however, it's easier to just add a day and use the
        # next day as an excluded upper bound on the interval. So we actually select all data earlier than
        # 00:00:00.000 on the day following the last day in the interval.
        end_date = dates[-1] + datetime.timedelta(1)
        return (
            mysql_datetime(start_date),
            mysql_datetime(end_date)
        )

    @property
    def filename(self):
        return 'user_registrations_{0}.tsv'.format(self.date_interval)
