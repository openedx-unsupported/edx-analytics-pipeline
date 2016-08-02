"""
Mixin class that defines shared parameters
Calculates interval from number of weeks and an end date.
"""

import datetime

import luigi
import luigi.date_interval

from edx.analytics.tasks.util import Week


class WeeklyIntervalMixin(object):
    end_date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='A day within the upper bound week. The week that contains this date will *not* be included '
        'in the analysis, however, all of the data up to the first day of this week will be included. This is '
        'consistent with all of our existing closed-open intervals. Default is today, UTC.',
    )
    weeks = luigi.IntParameter(
        default=24,
        description='The number of weeks to include in the analysis, counting back from the week that contains '
        'the end_date.',
    )

    @property
    def interval(self):
        """Given the parameters, compute the first and last date of the interval."""

        if self.weeks == 0:
            raise ValueError('Number of weeks to process must be greater than 0')

        starting_week = self.get_iso_week_containing_date(self.end_date - datetime.timedelta(weeks=self.weeks))
        ending_week = self.get_iso_week_containing_date(self.end_date)

        # include all complete weeks up to but not including the week containing the end_date
        return luigi.date_interval.Custom(starting_week.monday(), ending_week.monday())

    def get_iso_week_containing_date(self, date):
        """Returns a Week object corresponding to the given date."""
        iso_year, iso_weekofyear, _iso_weekday = date.isocalendar()
        return Week(iso_year, iso_weekofyear)
