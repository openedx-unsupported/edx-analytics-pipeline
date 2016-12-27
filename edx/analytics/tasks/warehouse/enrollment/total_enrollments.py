"""Total Enrollment related reports"""

import csv
from datetime import timedelta, date

import luigi
import luigi.hdfs

from luigi.date_interval import Custom

import numpy
import pandas

from edx.analytics.tasks.util.tsv import read_tsv
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.user_registrations import UserRegistrationsPerDay
from edx.analytics.tasks.reports.enrollments import CourseEnrollmentCountMixin
from edx.analytics.tasks.course_enroll import CourseEnrollmentChangesPerDay

import logging
log = logging.getLogger(__name__)

MINIMUM_DATE = date(1900, 1, 1)


class AllCourseEnrollmentCountMixin(CourseEnrollmentCountMixin):

    ROWNAME_HEADER = ' '

    def read_date_count_tsv(self, input_file):
        """
        Read TSV containing dates and corresponding counts into a pandas Series.

        NANs are not filled in here, as more than one filling strategy is
        used with such files.
        """
        names = ['date', 'count']

        data = read_tsv(input_file, names)
        data.date = pandas.to_datetime(data.date)
        data = data.set_index('date')

        # Ensure a continuos date range
        date_range = pandas.date_range(min(data.index), max(data.index))
        data = data.reindex(date_range)

        # Return as a Series
        return data['count']

    def read_incremental_count_tsv(self, input_file):
        """
        Read TSV containing dates and incremental counts.

        Args:
            input_file:  TSV file with dates and incremental counts.

        Returns:
            pandas Series containing daily counts.  Counts for missing days are set to zero.
        """
        return self.read_date_count_tsv(input_file).fillna(0)

    def read_total_count_tsv(self, input_file):
        """
        Read TSV containing dates and total counts.

        Args:
            input_file:  TSV file with dates and total counts.

        Returns:
            pandas Series containing daily counts.  Counts for missing days are interpolated.
        """
        return self.read_date_count_tsv(input_file).interpolate(method='time')

    def read_course_blacklist(self):
        """
        Reads a set of course_ids from the blacklist input file if one was
        specified, otherwise returns an empty set.

        Expected input file format is a single course ID per line.

        Returns:
            A set of course_ids that should not be included in aggregates.
        """
        if self.input().get('blacklist'):
            with self.input()['blacklist'].open('r') as blacklist_file:
                data = read_tsv(blacklist_file, ['course_id'])
            return set(data['course_id'])
        else:
            return set()

    def filter_out_courses(self, course_data, course_blacklist):
        """
        Removes data for courses that should be excluded from aggregates.

        Args:
            course_data (pandas.DataFrame): A DataFrame containing a single
                column for each course.
            course_blacklist (iterable): A collection of course IDs to
                remove from the data table.

        Returns:
            None, the `course_data` is modified in place.
        """
        for course_id in course_blacklist:
            try:
                # Drop from axis 1 because we are dropping columns, not rows.
                course_data.drop(course_id, axis=1, inplace=True)
            except ValueError:
                # There is no column for this course.
                pass

    def save_output(self, results, output_file):
        """
        Write output to CSV file.

        Args:
            results:  a pandas DataFrame object containing series data
                per row to be output.

        """
        # transpose the dataframe so that weeks are columns, and output:
        results = results.transpose()

        # List of fieldnames for the report
        fieldnames = [self.ROWNAME_HEADER] + list(results.columns)

        writer = csv.DictWriter(output_file, fieldnames)
        writer.writerow(dict((k, k) for k in fieldnames))  # Write header

        def format_counts(counts_dict):
            """Replace NaN with dashes."""
            for k, v in counts_dict.iteritems():
                yield k, '-' if numpy.isnan(v) else int(v)

        for series_name, series in results.iterrows():
            values = {
                self.ROWNAME_HEADER: series_name,
            }
            by_week_values = format_counts(series.to_dict())
            values.update(by_week_values)
            writer.writerow(values)


class WeeklyAllUsersAndEnrollments(luigi.Task, AllCourseEnrollmentCountMixin):
    """
    Calculates total users and enrollments across all (known) courses per week.

    Returns:
        Excel-compatible CSV file with a header row and two non-header
        rows.  The first column is a title for the row, and subsequent
        columns are the total counts for each week requested.  The
        first non-header row contains the total users at the end of
        each week.  The second row contains the total course
        enrollments at the end of each week.

    """

    ROW_LABELS = {
        'header': ' ',
        'registrations': 'Total Users',
        'enrollments': 'Course Enrollments',

    }

    @property
    def start_date(self):
        """
        Returns:
            The first date to include in the result.
        """
        return self.date - timedelta(self.weeks * 7)

    def requires(self):
        # The end date is not included in the result, so we have to add a day
        # to the provided date in order to ensure user registration data is
        # gathered for that date.
        end_date = self.date + timedelta(1)

        # In order to compute the cumulative sum of user registrations we need
        # all changes in registrations up to (and including) the provided date.
        registrations = UserRegistrationsPerDay(
            credentials=self.credentials,
            destination=self.destination,
            date_interval=Custom(MINIMUM_DATE, end_date)
        )

        results = {
            'enrollments': CourseEnrollmentChangesPerDay(
                name=self.name,
                src=self.src,
                dest=self.destination,
                include=self.include,
                manifest=self.manifest,
                mapreduce_engine=self.mapreduce_engine,
                lib_jar=self.lib_jar,
                n_reduce_tasks=self.n_reduce_tasks
            ),

            'registrations': registrations
        }
        if self.offsets:
            results.update({'offsets': ExternalURL(self.offsets)})
        if self.history:
            results.update({'history': ExternalURL(self.history)})
        if self.blacklist:
            results.update({'blacklist': ExternalURL(self.blacklist)})

        return results

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.destination,
                'total_users_and_enrollments_{0}-{1}.csv'.format(self.start_date, self.date)
            )
        )

    def run(self):
        # Load the explicit enrollment data into a pandas dataframe.
        daily_enrollment_changes = self.read_enrollments()

        # Add enrollment offsets to allow totals to be calculated
        # for explicit enrollments.
        offsets = self.read_offsets()
        daily_enrollment_totals = self.calculate_total_enrollment(daily_enrollment_changes, offsets)
        course_blacklist = self.read_course_blacklist()
        self.filter_out_courses(daily_enrollment_totals, course_blacklist)

        # Sum per-course counts to create a single series
        # of total enrollment counts per day.
        daily_overall_enrollment = daily_enrollment_totals.sum(axis=1)
        # Prepend total enrollment history.
        overall_enrollment_history = self.read_history()
        if overall_enrollment_history is not None:
            daily_overall_enrollment = self.prepend_history(daily_overall_enrollment, overall_enrollment_history)

        daily_overall_enrollment.name = self.ROW_LABELS['enrollments']

        daily_user_registration_totals = self.read_user_registrations()

        # Because the registration data index is the requested date range
        # use it as the canonical index and left join in the enrollment
        # counts.
        total_counts_by_day = pandas.merge(
            daily_user_registration_totals,
            pandas.DataFrame(daily_overall_enrollment),
            how='left',
            left_index=True,
            right_index=True
        )

        # Select values from DataFrame to display per-week.
        total_counts_by_week = self.select_weekly_values(
            total_counts_by_day,
            self.date,
            self.weeks,
        )

        with self.output().open('w') as output_file:
            self.save_output(total_counts_by_week, output_file)

    def read_enrollments(self):
        """
        Read enrollments into a pandas DataFrame.

        Returns:
            Pandas dataframe with one column per course_id. Indexed
            for the time interval available in the enrollments data.

        """
        with self.input()['enrollments'].open('r') as input_file:
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

    def read_history(self):
        """
        Read course total enrollment history into a pandas DataFrame.

        Returns:
            Pandas Series, indexed by date, containing total
            enrollment counts by date.

            Returns None if no history was specified.
        """
        data = None
        if self.input().get('history'):
            with self.input()['history'].open('r') as history_file:
                data = self.read_total_count_tsv(history_file)
        return data

    def read_user_registrations(self):
        """
        Read history of user registrations.

        Returns:
            Pandas DataFrame indexed by date with a single column
            representing the number of users who have accounts at
            the end of that day.
        """
        with self.input()['registrations'].open('r') as registrations_file:
            # The column name here will be converted in to a row name later when
            # the data is transposed.
            registration_changes = read_tsv(registrations_file, ['date', self.ROW_LABELS['registrations']])
            registration_changes.date = pandas.to_datetime(registration_changes.date)
            registration_changes.set_index(['date'], inplace=True)

            cumulative_registrations = registration_changes.cumsum()

            # Restrict the index to only the date range requested
            date_range = pandas.date_range(self.start_date, self.date)
            # Forward fill gaps because those dates have no change in registrations
            cumulative_registrations = cumulative_registrations.reindex(date_range, method='ffill')

        return cumulative_registrations

    def prepend_history(self, count_by_day, history):
        """
        Add history to a series in-place.

        Args:
            count_by_day: pandas Series
            history: pandas Series, also of counts indexed by date.

        """
        # Get history dates that are not in the regular count data so there is no overlap.
        last_day_of_history = count_by_day.index[0] - timedelta(1)
        truncated_history = history[:last_day_of_history]
        result = count_by_day.append(truncated_history, verify_integrity=True)

        return result
