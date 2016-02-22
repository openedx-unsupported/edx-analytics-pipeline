"""Reports about Incremental enrollment."""

from datetime import timedelta, date

import luigi
import luigi.hdfs
from luigi.date_interval import Custom
import pandas
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join
from edx.analytics.tasks.reports.total_enrollments import AllCourseEnrollmentCountMixin
from edx.analytics.tasks.course_enroll import CourseEnrollmentChangesPerDay
from edx.analytics.tasks.user_registrations import UserRegistrationsPerDay

MINIMUM_DATE = date(1900, 1, 1)


class WeeklyIncrementalUsersAndEnrollments(luigi.Task, AllCourseEnrollmentCountMixin):
    """
    Calculates weekly incremental changes in users and enrollments across courses.

    Returns:
        Excel-compatible CSV file with a header row and four
        non-header rows.  The first column is a title for the row, and
        subsequent columns are the incremental counts for each week
        requested.  The first non-header row contains the change in
        registered users during each week, and the second calculates
        the daily average change in users.  The third row contains the
        change in total course enrollments during each week, and the
        fourth row again averages this for a per-day average change in
        course enrollments.

    """

    ROW_LABELS = {
        'header': ' ',
        'registration_change': 'New User Registrations',
        'average_registration_change': 'Average New User Changes',
        'enrollment_change': 'Course Enrollment Increment',
        'average_enrollment_change': 'Average Daily Course Enrollment Increments',
    }

    def requires(self):
        end_date = self.date + timedelta(1)
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
            'registrations': UserRegistrationsPerDay(
                credentials=self.credentials,
                destination=self.destination,
                date_interval=Custom(MINIMUM_DATE, end_date)
            ),
        }
        if self.blacklist:
            results.update({'blacklist': ExternalURL(self.blacklist)})
        return results

    def output(self):
        return get_target_from_url(
            url_path_join(
                self.destination,
                "incremental_users_and_enrollments_{0}.csv".format(self.name)
            )
        )

    def run(self):
        # Load the user registration data into a pandas dataframe.
        with self.input()['registrations'].open('r') as input_file:
            daily_registration_changes = self.read_incremental_count_tsv(input_file)

        # Load the explicit enrollment data into a pandas dataframe.
        daily_enrollment_changes = self.read_enrollments()

        course_blacklist = self.read_course_blacklist()
        self.filter_out_courses(daily_enrollment_changes, course_blacklist)

        # Sum per-course counts to create a single series
        # of total enrollment counts per day.
        daily_overall_enrollment = daily_enrollment_changes.sum(axis=1)

        # Roll up values from DataFrame into per-week sums.
        weekly_registration_changes = self.aggregate_per_week(
            daily_registration_changes,
            self.date,
            self.weeks,
        )

        weekly_overall_enrollment = self.aggregate_per_week(
            daily_overall_enrollment,
            self.date,
            self.weeks,
        )

        # Gather all required series into a single DataFrame
        # in the form it should take for output:
        weekly_report = self.assemble_report_dataframe(
            weekly_registration_changes, weekly_overall_enrollment
        )

        with self.output().open('w') as output_file:
            self.save_output(weekly_report, output_file)

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

    def aggregate_per_week(self, daily_values, last_week_ending, weeks):
        """
        Aggregates daily values into weekly values.

        Args:
            daily_values: Pandas Series of daily values, indexed by date.
                All dates are assumed to be contiguous, though their values may be NaN.
                Dates do not have to cover the periods being sampled.
            last_week_ending: last day of last week.
            weeks: number of weeks to sample (including the last day)

        Returns:
            Pandas Series with weekly values, indexed by date of last day of week.
            Any day with NaN will result in the corresponding week also being NaN.
            As a consequence, any week requested that is not completely covered
            by the input daily_values will be NaN.
        """
        # For each date in daily input, find sum of day's value with the previous
        # six days.
        week_window = pandas.rolling_sum(daily_values, window=7)

        # Pull out the requested end-of-week days.  If requested week dates are
        # not in the range of the daily input, NaN values are returned.
        days = [last_week_ending - timedelta(i * 7) for i in reversed(xrange(weeks))]
        return week_window.loc[days]

    @staticmethod
    def row_label(row_name):
        """Returns label value for reference row, given its internal row name."""
        return WeeklyIncrementalUsersAndEnrollments.ROW_LABELS[row_name]

    def assemble_report_dataframe(self, weekly_registration_changes, weekly_enrollment_changes):
        """
        Create a dataframe that represents the final report.

        Args:
            weekly_registration_changes:  Pandas series, with date as index.
            weekly_enrollment_changes:  Pandas series, with date as index.

        Returns:
            A Pandas dataframe, with date as index and four columns.
        """

        weekly_report = pandas.DataFrame(
            {
                self.row_label('registration_change'): weekly_registration_changes,
                self.row_label('average_registration_change'): weekly_registration_changes / 7.,
                self.row_label('enrollment_change'): weekly_enrollment_changes,
                self.row_label('average_enrollment_change'): weekly_enrollment_changes / 7.,
            },
            columns=[
                self.row_label('registration_change'),
                self.row_label('average_registration_change'),
                self.row_label('enrollment_change'),
                self.row_label('average_enrollment_change'),
            ]
        )
        return weekly_report


class DailyRegistrationsEnrollmentsAndCourses(luigi.Task,
                                              AllCourseEnrollmentCountMixin):
    """
    Calculates users registration and total enrollments across courses.

    Returns:
        Excel-compatible CSV file with a header row.
        Columns are the days requested.
        First row is number of user registrations.
        Second row is the number of total course enrollments.
        Third row is the number of live courses.

    """

    ROW_LABELS = {
        'header': ' ',
        'registrations': 'Daily New Users',
        'enrollments': 'Daily Course Enrollment Changes',
        'courses': 'Total Live Courses',
    }

    @property
    def date_range(self):
        """The date range according to the task parameters"""
        end = self.date
        start = end - timedelta(self.days)
        date_range = pandas.date_range(start, end, closed='right')
        return date_range.date

    def requires(self):
        end_date = self.date + timedelta(1)
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
            'registrations': UserRegistrationsPerDay(
                credentials=self.credentials,
                destination=self.destination,
                date_interval=Custom(MINIMUM_DATE, end_date)
            ),
        }
        if self.blacklist:
            results.update({'blacklist': ExternalURL(self.blacklist)})
        return results

    def output(self):
        return get_target_from_url(
            url_path_join(self.destination, "daily_registrations_enrollments_{0}.csv".format(self.name))
        )

    def run(self):
        daily_registrations = self.read_registrations()
        daily_enrollments = self.read_enrollments()

        report = self.assemble_report(
            daily_registrations,
            daily_enrollments,
        )

        with self.output().open('w') as output_file:
            self.save_output(report, output_file)

    def read_registrations(self):
        """
        Read history of user registrations.

        Returns:
            Pandas DataFrame indexed by date with a single column
            representing the number of users who have accounts at
            the end of that day.
        """
        with self.input()['registrations'].open('r') as input_file:
            daily_registration_changes = self.read_incremental_count_tsv(input_file)
        return daily_registration_changes.reindex(self.date_range)

    def read_enrollments(self):
        """
        Read enrollments into a pandas DataFrame.

        Returns:
            Pandas dataframe with one column per course_id. Indexed
            for the time interval available in the enrollments data.

        """
        with self.input()['enrollments'].open('r') as input_file:
            course_date_count_data = self.read_course_date_count_tsv(input_file)
            enrollments = self.initialize_daily_count(course_date_count_data)

        course_blacklist = self.read_course_blacklist()
        self.filter_out_courses(enrollments, course_blacklist)

        # Sum counts for all courses
        result = enrollments.sum(axis=1)

        return result.reindex(self.date_range)

    def assemble_report(self, registrations, enrollments):
        """
        Create a dataframe that represents the final report.

        Args:
            registration:  Pandas series, with date as index.
            enrollments:  Pandas series, with date as index.

        Returns:
            A Pandas dataframe, with date as index and two columns.
        """

        report = pandas.DataFrame.from_items([
            (self.ROW_LABELS['registrations'], registrations),
            (self.ROW_LABELS['enrollments'], enrollments),
        ])
        return report
