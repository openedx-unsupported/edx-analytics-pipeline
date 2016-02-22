"""Enrollment related reports"""

import csv
from datetime import timedelta, date

import luigi
import luigi.hdfs

import numpy
import pandas

from edx.analytics.tasks.util.tsv import read_tsv
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.course_enroll import CourseEnrollmentChangesPerDay
from edx.analytics.tasks.mapreduce import MapReduceJobTaskMixin
from edx.analytics.tasks.util.opaque_key_util import get_org_id_for_course

DEFAULT_NUM_WEEKS = 52
DEFAULT_NUM_DAYS = 28


class CourseEnrollmentCountMixin(MapReduceJobTaskMixin):
    """ Provides common parameters used in executive report tasks """
    name = luigi.Parameter()
    src = luigi.Parameter(
        is_list=True,
        config_path={'section': 'enrollment-reports', 'name': 'src'},
        description='Location of daily enrollments per date. The format is a '
        'Hadoop TSV file, with fields `course_id`, `date` and `count`.',
    )
    include = luigi.Parameter(is_list=True, default=('*',))
    weeks = luigi.IntParameter(
        default=DEFAULT_NUM_WEEKS,
        description='Number of weeks from the end date to request.',
    )
    days = luigi.Parameter(
        default=DEFAULT_NUM_DAYS,
        description='Number of days from the end date to request.',
    )
    offsets = luigi.Parameter(
        default=None,
        description='Location of seed values for each course. The format is a '
        'Hadoop TSV file, with fields `course_id`, `date` and `offset`.',
    )
    history = luigi.Parameter(
        default=None,
        description='Location of historical values for total course enrollment. '
        'The format is a TSV file, with fields `date` and `enrollments`.',
    )
    date = luigi.DateParameter(
        default=date.today(),
        description='End date of the last week requested. Default is today.',
    )
    statuses = luigi.Parameter(default=None)
    manifest = luigi.Parameter(default=None)
    manifest_path = luigi.Parameter(default=None)
    destination_directory = luigi.Parameter(
        default=None,
        description='Directory to store the resulting report and intermediate '
        'results. The output format is an excel-compatible CSV file.',
    )
    destination = luigi.Parameter(
        config_path={'section': 'enrollment-reports', 'name': 'destination'},
        description='Location of the resulting report. The output format is a '
        'Excel-compatible CSV file with `course_id` and one column per requested week.',
    )
    credentials = luigi.Parameter(
        config_path={'section': 'database-import', 'name': 'credentials'}
    )
    blacklist = luigi.Parameter(config_path={'section': 'enrollment-reports', 'name': 'blacklist'})

    """Provides methods useful for generating reports using course enrollment counts."""

    def read_course_date_count_tsv(self, input_file):
        """Read TSV file with hard-coded column names into a pandas DataFrame."""
        names = ['course_id', 'date', 'count']

        # Not assuming any encoding, course_id will be read as plain string
        data = read_tsv(input_file, names)

        data.date = pandas.to_datetime(data.date)
        return data

    def initialize_daily_count(self, course_date_count_data):
        """
        Reorganize a course-date-count data table to index by date.

        Args:
            Pandas dataframe with one row per course_id and
            columns for the date and count of the offset.

        Returns:
            Pandas dataframe with one column per course_id, and
            indexed rows for the date.  Counts are set to zero for
            dates that are missing.

        """
        data = course_date_count_data.pivot(
            index='date',
            columns='course_id',
            values='count',
        )

        # Complete the range of data to include all days between
        # the dates of the first and last events.
        date_range = pandas.date_range(min(data.index), max(data.index))
        data = data.reindex(date_range)
        data = data.fillna(0)

        return data

    def add_offsets_to_daily_count(self, count_by_day, offsets):
        """
        Add offsets to a dataframe in-place.

        Args:
            count_by_day: Pandas dataframe with one column per course_id, and
                indexed rows for the date.
            offsets: Pandas dataframe with one row per course_id and
                columns for the date and count of the offset.

        """
        for _, (course_id, date, count) in offsets.iterrows():
            if course_id in count_by_day.columns:
                # The offsets are computed to beginning of that day. We
                # add them to the counts by the end of that day to
                # get the correct count for the day.
                count_by_day.loc[date, course_id] += count
            else:
                # We have an offset for the course, but no current
                # counts.  Create an course entry, set the offset, and set
                # all subsequent counts to zero.
                count_by_day.loc[date, course_id] = count
                count_by_day.loc[count_by_day.index > date, course_id] = 0

            # Flag values before the offset day with NaN,
            # since they are not "available".
            not_available = count_by_day.index < date
            count_by_day.loc[not_available, course_id] = numpy.NaN

    def calculate_total_enrollment(self, count_by_day, offsets=None):
        """
        Accumulate enrollment changes per day to find total enrollment per day.

        Args:
            count_by_day: Pandas dataframe with one column per course_id, and
                indexed rows for the date.  Counts are net changes in enrollment
                during the day for each course.
            offsets: Pandas dataframe with one row per course_id and
                columns for the date and count of the offset.  The offset
                for a course is used to provide total enrollment counts
                at a point in time right before the timeframe covered by count_by_day.

        """
        if offsets is not None:
            self.add_offsets_to_daily_count(count_by_day, offsets)
        # Calculate the cumulative sum per day of the input.
        # Entries with NaN stay NaN.
        # At this stage only the data prior to the offset should contain NaN.
        cumulative_sum = count_by_day.cumsum()
        return cumulative_sum

    def select_weekly_values(self, daily_values, start, weeks):
        """
        Sample daily values on a weekly basis.

        Args:
            daily_values: Pandas dataframe with one column per course_id, and
                indexed rows for the date.
            start: last day to request.
            weeks: number of weeks to sample (including the last day)
        """
        # List the dates of the last day of each week requested.
        days = [start - timedelta(i * 7) for i in reversed(xrange(0, weeks))]

        # Sample the cumulative data on the requested days.
        # Result is NaN if there is no data available for that date.
        results = daily_values.loc[days]

        return results


class EnrollmentsByWeek(luigi.Task, CourseEnrollmentCountMixin):
    """Calculates cumulative enrollments per week per course.

    Returns:
        Excel CSV file with one row per course. The columns are
        the cumulative enrollments counts for each week requested.

    """

    def requires(self):
        results = {
            'source': CourseEnrollmentChangesPerDay(
                name=self.name,
                src=self.src,
                dest=self.destination,
                include=self.include,
                manifest=self.manifest,
                mapreduce_engine=self.mapreduce_engine,
                lib_jar=self.lib_jar,
                n_reduce_tasks=self.n_reduce_tasks
            )
        }
        if self.offsets:
            results.update({'offsets': ExternalURL(self.offsets)})
        if self.statuses:
            results.update({'statuses': ExternalURL(self.statuses)})

        return results

    def output(self):
        return get_target_from_url(url_path_join(self.destination, "weekly_enrollments_{0}.csv".format(self.name)))

    def run(self):
        # Load the data into pandas dataframes
        daily_enrollment_changes = self.read_source()
        offsets = self.read_offsets()

        daily_enrollment_totals = self.calculate_total_enrollment(daily_enrollment_changes, offsets)

        # Sample the cumulative data on the requested days.
        # Result is NaN if there is no data available for that date.
        weekly_enrollment_totals = self.select_weekly_values(
            daily_enrollment_totals,
            self.date,
            self.weeks
        )

        statuses = self.read_statuses()

        with self.output().open('w') as output_file:
            self.save_output(weekly_enrollment_totals, statuses, output_file)

    def read_source(self):
        """
        Read source into a pandas DataFrame.

        Returns:
            Pandas dataframe with one column per course_id. Indexed
            for the time interval available in the source data.

        """
        with self.input()['source'].open('r') as input_file:
            course_date_count_data = self.read_course_date_count_tsv(input_file)
            data = self.initialize_daily_count(course_date_count_data)

        return data

    def read_offsets(self):
        """
        Read offsets into a pandas DataFrame.

        Returns:
            Pandas dataframe with one row per course_id and
            columns for the date and count of the offset.

            Returns None if no offset was specified.

        """
        data = None

        if self.input().get('offsets'):
            with self.input()['offsets'].open('r') as offset_file:
                data = self.read_course_date_count_tsv(offset_file)

        return data

    def read_statuses(self):
        """
        Read course statuses into a pandas DataFrame.

        Returns:
            Pandas dataframe with one row per course_id and
            a column for the status. The status should
            be either "past", "current" or "new".  The index
            for the DataFrame is the course_id.

            Returns None if no statuses was specified.
        """
        data = None
        names = ['course_id', 'status']

        if self.input().get('statuses'):
            with self.input()['statuses'].open('r') as status_file:
                data = read_tsv(status_file, names)
                data = data.set_index('course_id')

        return data

    def save_output(self, results, statuses, output_file):
        results = results.transpose()

        # List of fieldnames for the report
        fieldnames = ['status', 'course_id', 'org_id'] + list(results.columns)

        writer = csv.DictWriter(output_file, fieldnames)
        writer.writerow(dict((k, k) for k in fieldnames))  # Write header

        def format_counts(counts_dict):
            for k, v in counts_dict.iteritems():
                yield k, '-' if numpy.isnan(v) else int(v)

        for course_id, series in results.iterrows():
            # Course_id is passed throughout these reports as a
            # utf8-encoded str, so it must be locally converted to
            # unicode before parsing for org.
            org_id = get_org_id_for_course(course_id.decode('utf-8'))

            values = {
                'course_id': course_id,
                'status': self.get_status_for_course(course_id, statuses),
                'org_id': org_id or '-',
            }
            by_week_values = format_counts(series.to_dict())
            values.update(by_week_values)
            writer.writerow(values)

    def get_status_for_course(self, course_id, statuses):
        '''
        Args:
            course_id(str): The identifier for the course.  Should be formatted
                as <org_id>/<name>/<run>.
            statuses(pandas.DataFrame): A pandas DataFrame mapping course_ids
                to course statuses.  It is expected to be indexed on course_id.

        Returns:
            The course's status as a string.
        '''
        if statuses is None or course_id not in statuses.index:
            return '-'

        return statuses.loc[course_id]['status']
