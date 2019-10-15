import csv
import datetime
import logging

import luigi
from luigi.util import inherits

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.vertica_export import (
    VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, ExportVerticaTableToS3Task, get_vertica_table_schema
)
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class ProgramsReportTaskMixin(object):
    vertica_credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    output_root = luigi.Parameter(
        description='URL pointing to the location reports should be stored',
    )
    vertica_schema_name = luigi.Parameter(
        default='programs_reporting',
        description='Vertica schema containing reporting table',
    )
    table_name = luigi.Parameter(
        default='learner_enrollments',
        description='Table containing enrollment rows to report on',
    )
    vertica_warehouse_name = luigi.Parameter(
        default='docker',
        description='The Vertica warehouse that houses the report schema.',
    )
    sqoop_null_string = luigi.Parameter(
        default='null',
        description='A string replacement value for any (null) values encountered by Sqoop when exporting from Vertica.',
    )
    sqoop_fields_terminated_by = luigi.Parameter(
        default='\t',
        description='The field delimiter used by Sqoop.',
    )
    sqoop_delimiter_replacement = luigi.Parameter(
        default=' ',
        description='The string replacement value for special characters encountered by Sqoop when exporting from '
                    'Vertica.',
    )
    overwrite = luigi.BoolParameter(
        default=True,
        description='Whether or not to overwrite existing outputs',
    )
    overwrite_export = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite existing database export'
    )


class BaseProgramReportsTask(OverwriteOutputMixin, ProgramsReportTaskMixin, MultiOutputMapReduceJobTask):
    """ Generates CSV reports on program enrollment """

    report_name = luigi.Parameter(
        description='Name of report file(s) to output'
    )
    date = luigi.Parameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date. Used to tag report date'
    )

    def __init__(self, *args, **kwargs):
        super(BaseProgramReportsTask, self).__init__(*args, **kwargs)
        self.columns = self.get_column_names()

    def requires(self):
        return ExportVerticaTableToS3Task(
            vertica_schema_name=self.vertica_schema_name,
            table_name=self.table_name,
            vertica_credentials=self.vertica_credentials,
            vertica_warehouse_name=self.vertica_warehouse_name,
            sqoop_null_string=self.sqoop_null_string,
            sqoop_fields_terminated_by=self.sqoop_fields_terminated_by,
            sqoop_delimiter_replacement=self.sqoop_delimiter_replacement,
            overwrite=(self.overwrite_export and self.overwrite),
        )

    def get_column_names(self):
        """
        List names of columns as they should appear in the CSV.

        This must match the order they are stored in the exported warehouse table
        """
        return []  # should be implemented by child

    def multi_output_reducer(self, key, values, output_file):
        pass  # should be implemented by child

    def run(self):
        """
        Clear out output if overwrite requested.
        """
        self.remove_output_on_overwrite()
        super(BaseProgramReportsTask, self).run()

    def mapper(self, line):
        """
        Group input by authoring institution and program
        """
        (org, program_title, program_uuid, content) = line.split('\t', 3)
        yield (org, program_uuid), line

    def output_path_for_key(self, key):
        org_key, program_uuid = key
        filename = u'{}__{}.csv'.format(self.report_name, self.date)
        return url_path_join(self.output_root, org_key, program_uuid, filename)


class BuildLearnerProgramReportTask(BaseProgramReportsTask):

    report_name = luigi.Parameter(
        default='learner_report',
        description='Name of report file(s) to output'
    )
    date = luigi.Parameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date. Used to tag report date'
    )

    @staticmethod
    def get_column_names():
        """
        List names of columns as they should appear in the CSV.

        This must match the order they are stored in the exported warehouse table
        """
        return [
            'Authoring Institution',
            'Program Title',
            'Program UUID',
            'Program Type',
            'User ID',
            'Username',
            'Name',
            'User Key',
            'Course Title',
            'Course Run Key',
            'External Course Key',
            'Track',
            'Grade',
            'Letter Grade',
            'Date First Enrolled',
            'Date Last Unenrolled',
            'Currently Enrolled',
            'Date First Upgraded to Verified',
            'Completed',
            'Date Completed'
        ]

    def multi_output_reducer(self, key, values, output_file):
        """
        Map export values to report output fields and write to csv.  Drops any extra columns
        """
        writer = csv.DictWriter(output_file, self.columns)
        writer.writerow(dict(
            (k, k) for k in self.columns
        ))

        for content in values:
            fields = content.split('\t')
            row = {field_key: field_value for field_key, field_value in zip(self.columns, fields)}
            writer.writerow(row)


class RemoveOutputMixin(object):

    def run(self):
        """
        Clear out output if overwrite requested.
        """
        self.remove_output_on_overwrite()
        super(RemoveOutputMixin, self).run()


class CombineCourseEnrollmentsTask(OverwriteOutputMixin, RemoveOutputMixin, MapReduceJobTask):
    """
        A Map Reduce task that combines multiple course run enrollment records for a single course into a single
        record per course. It does some aggregation on data that may differ across multiple enrollments in the same course.

        The distinct enrollment tracks are joined together.
        The year of the earliest enrollment in any run of a course is used as the learner's entry year in that course. If there is no
        enrollment date information for all runs, the learner's entry year is none.
        The course is considered completed by the user if at least one run of the course is completed.
    """
    date = luigi.Parameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date. Used to tag report date'
    )
    output_root = luigi.Parameter(
        description='URL pointing to the location reports should be stored',
    )
    overwrite = luigi.BoolParameter(
        default=True,
        description='Whether or not to overwrite existing outputs',
    )
    overwrite_export = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite existing database export'
    )
    vertica_schema_name = luigi.Parameter(
        default='programs_reporting',
        description='Vertica schema containing reporting table',
    )
    sqoop_null_string = luigi.Parameter(
        default='null',
        description='A string replacement value for any (null) values encountered by Sqoop when exporting from Vertica.',
    )

    def requires(self):
        return self.clone(ExportVerticaTableToS3Task, overwrite=(self.overwrite_export and self.overwrite))

    def mapper(self, line):
        """Yield a (key, value) tuple for each course run enrollment record."""
        fields = line.split(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii'))

        authoring_org = fields[0]
        program_title = fields[1]
        program_uuid = fields[2]
        program_type = fields[3]
        user_id = fields[4]
        course_key = fields[21]
        timestamp = fields[22]

        yield (authoring_org, program_type, program_uuid, program_title, user_id, course_key, timestamp), line

    def reducer(self, key, values):
        """
            For a given key, representing a leaner's enrollment in a particular course as part of a program at a particular time,
            do some aggregation on the values, which repesent course run enrollments in said course.

            In particular,
                - combine the different distinct enrollment tracks the learner has been enrolled in into a comma separated string
                - find the earliest enrollment time across all course run enrollments for that course and use the year as the learner's entry year
                    - if all values are null, indicating missing data, use null as the entry year
                - determine whether the learner completed the course by checking whether the learner has completed at least one run of the course

            Yield a (key, value) pair where the key is empty, and the value represents information about a learner's enrollment in a course.
        """
        entry_years = set()
        completed = False
        program_completed = False
        tracks = set()

        authoring_org, program_type, program_uuid, program_title, user_id, course_key, timestamp = key

        for value in values:
            fields = value.split(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii'))

            program_completed = string_to_bool(fields[20])

            track = fields[11]
            tracks.add(track)

            is_course_run_completed = string_to_bool(fields[18])
            completed = completed or is_course_run_completed

            if fields[14] != self.sqoop_null_string:
                year = datetime.datetime.strptime(fields[14], '%Y-%m-%d %H:%M:%S.%f').year
                entry_years.add(year)

        # the method that writes the output of the reducers to a file writes the elements of the output as a tab separated string,
        # so to keep the list of tracks together, use a comma separated string
        tracks = ','.join(tracks)

        # if all course run enrollments have null for their first enrollment time, use null
        # find the minimum value for the entry year excluding None; if None is the only value,
        # then entry year is 'null'
        if len(entry_years) > 0:
            entry_year = min(entry_years)
        else:
            entry_year = 'null'

        yield [authoring_org, program_type, program_title, program_uuid, user_id, tracks, entry_year, completed, program_completed, timestamp]

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp', 'CombineCourseEnrollments/'))


class CountCourseEnrollmentsTask(OverwriteOutputMixin, RemoveOutputMixin, MapReduceJobTask):
    """
        A Map Reduce task that counts a learner's course enrollments as part of a program. In particular, it counts
            - the total number of course enrollments
            - the number of completed courses
            - the number of courses in which the learner is enrolled in the following tracks
                - audit
                - verified
                - professional or no-id-professional
                - masters

        It also includes whether the learner has completed the program.
    """

    def requires(self):
        return self.clone(CombineCourseEnrollmentsTask)

    def mapper(self, line):
        """Yield a (key, value) tuple for each learner enrolled in a program."""

        # although we originally split on the VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER,
        # the writer of the previous reduce task uses a tab delimeter
        fields = line.split('\t')

        authoring_org = fields[0]
        program_type = fields[1]
        program_title = fields[2]
        program_uuid = fields[3]
        user_id = fields[4]
        entry_year = fields[6]
        timestamp = fields[9]

        yield (authoring_org, program_type, program_title, program_uuid, user_id, entry_year, timestamp), line

    def reducer(self, key, values):
        """
            For a given key, representing a learner enrolled in a program at a particular time,
            do some aggregation on the values, which repesent course enrollments in said program.

            In particular, count
                - the total number of course enrollments
                - the number of completed courses
                - the number of courses in which the learner is enrolled in the following tracks
                    - audit
                    - verified
                    - professional or no-id-professional
                    - masters

            Also include whether the learner has completed the program.
        """
        authoring_org, program_type, program_title, program_uuid, user_id, entry_year, timestamp = key

        is_program_completed = False
        num_enrollments = 0
        num_completed_courses = 0
        num_audit_enrollments = 0
        num_verified_enrollments = 0
        num_professional_enrollments = 0
        num_masters_enrollents = 0

        for value in values:
            # although we originally split on the VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER,
            # the writer of the previous reduce task uses a tab delimeter
            fields = value.split('\t')

            num_enrollments += 1

            is_course_completed = string_to_bool(fields[7])
            if is_course_completed:
                num_completed_courses += 1

            is_program_completed = is_program_completed or string_to_bool(fields[8])

            tracks = fields[5].split(',')
            for track in tracks:
                if track == 'audit':
                    num_audit_enrollments += 1
                elif track == 'verified':
                    num_verified_enrollments += 1
                elif track == 'professional' or track == 'no-id-professional':
                    num_professional_enrollments += 1
                elif track == 'masters':
                    num_masters_enrollents += 1

        yield [authoring_org, program_type, program_title, program_uuid, user_id, entry_year, num_enrollments, num_completed_courses, num_audit_enrollments, num_verified_enrollments, num_professional_enrollments, num_masters_enrollents, is_program_completed, timestamp]

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp', 'CountCourseEnrollments/'))


class CountProgramCohortEnrollmentsTask(OverwriteOutputMixin, RemoveOutputMixin, MapReduceJobTask):
    """
        A Map Reduce task that counts a program's course enrollments. In particular, it counts
            - the total number of distinct learners
            - the total number of course enrollments
            - the numer of learners in 1+...10+ courses in the following tracks
                - audit
                - verified
                - professional or no-id-professional
                - masters
            - the number of learners with 1+...10+ completed courses
            - the number of learners who have completed the program
    """

    def requires(self):
        return self.clone(CountCourseEnrollmentsTask)

    def mapper(self, line):
        """Yield a (key, value) tuple for each program."""
        fields = line.split('\t')
        authoring_org = fields[0]
        program_type = fields[1]
        program_title = fields[2]
        program_uuid = fields[3]
        entry_year = fields[5]
        timestamp = fields[13]

        yield (authoring_org, program_type, program_title, program_uuid, entry_year, timestamp), line

    def reducer(self, key, values):
        """
            For a given key, representing a program at a particular time,
            do some aggregation on the values, which repesent enrollment data for said program.

            In particular, count
                - the total number of distinct learners
                - the total number of course enrollments
                - the numer of learners in 1+...10+ courses in the following tracks
                    - audit
                    - verified
                    - professional or no-id-professional
                    - masters
                - the number of learners with 1+...10+ completed courses
                - the number of learners who have completed the program
        """
        authoring_org, program_type, program_title, program_uuid, entry_year, timestamp = key

        total_num_learners = 0
        total_num_enrollments = 0
        total_num_program_completions = 0

        # TODO: make this dynamic based on the number of courses in a program; hard coded to 10
        # for now as overwhelming majority of programs have 10 or fewer courses
        num_courses = 10
        num_learners_in_audit = [0 for _ in range(num_courses)]
        num_learners_in_verified = [0 for _ in range(num_courses)]
        num_learners_in_professional = [0 for _ in range(num_courses)]
        num_learners_in_masters = [0 for _ in range(num_courses)]
        num_learners_completed_courses = [0 for _ in range(num_courses)]

        for value in values:
            fields = value.split('\t')

            total_num_learners += 1
            total_num_enrollments += int(fields[6])

            is_program_completed = fields[12]
            if string_to_bool(is_program_completed):
                total_num_program_completions += 1

            num_audit_enrollments = int(fields[8])
            num_verified_enrollments = int(fields[9])
            num_professional_enrollments = int(fields[10])
            num_masters_enrollments = int(fields[11])

            for num in range(num_audit_enrollments):
                num_learners_in_audit[num] += 1

            for num in range(num_verified_enrollments):
                num_learners_in_verified[num] += 1

            for num in range(num_professional_enrollments):
                num_learners_in_professional[num] += 1

            for num in range(num_masters_enrollments):
                num_learners_in_masters[num] += 1

            num_completed_courses = int(fields[7])

            for num in range(num_completed_courses):
                num_learners_completed_courses[num] += 1

        row = [authoring_org, program_type, program_title, program_uuid, entry_year, total_num_learners, total_num_enrollments]
        for num in num_learners_in_audit + num_learners_in_verified + num_learners_in_professional + num_learners_in_masters + num_learners_completed_courses:
            row.append(num)

        row.append(total_num_program_completions)
        row.append(timestamp)

        yield row

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp/CountProgramCohortEnrollments/'))


class BuildAggregateProgramReportTask(OverwriteOutputMixin, RemoveOutputMixin, MultiOutputMapReduceJobTask):
    """A Map Reduce task that writes a program's aggregate enrollment data to organization-program specific file."""

    report_name = luigi.Parameter(
        default='aggregate_report'
    )

    def __init__(self, *args, **kwargs):
        super(BuildAggregateProgramReportTask, self).__init__(*args, **kwargs)
        self.columns = self.get_column_names()

    def requires(self):
        return self.clone(CountProgramCohortEnrollmentsTask)

    @staticmethod
    def get_column_names():
        """
        List names of columns as they should appear in the CSV.
        """
        num_courses = 10

        columns = [
            'Authoring Institution',
            'Program Type',
            'Program Title',
            'Program UUID',
            'Entry Year',
            'Total Learners',
            'Total Number Enrollments',
        ]
        columns.extend(['Number of Learners in {}+ Audit'.format(num) for num in range(num_courses)])
        columns.extend(['Number of Learners in {}+ Verified'.format(num) for num in range(num_courses)])
        columns.extend(['Number of Learners in {}+ Professional'.format(num) for num in range(num_courses)])
        columns.extend(['Number of Learners in {}+ Master\'s'.format(num) for num in range(num_courses)])
        columns.extend(['Number of Learners Completed {}+ Courses'.format(num) for num in range(num_courses)])
        columns.append('Timestamp')
        return columns

    def output_path_for_key(self, key):
        authoring_institution, program_uuid = key
        filename = u'{}__{}.csv'.format(self.report_name, self.date)
        return url_path_join(self.output_root, authoring_institution, program_uuid, filename)

    def mapper(self, line):
        authoring_institution, _, _, program_uuid, _ = line.split('\t', 4)
        yield (authoring_institution, program_uuid), line

    def multi_output_reducer(self, key, values, output_file):
        writer = csv.DictWriter(output_file, self.columns)
        writer.writeheader()

        for value in values:
            fields = value.split('\t')

            row = {field_key: field_value for field_key, field_value in zip(self.columns, fields)}
            writer.writerow(row)


def string_to_bool(value):
    """
        Return boolean associated with string value.

       Arguments:
            value (str): a string representation of a boolean

        Returns:
            boolean: boolean value represented by value

        Raises:
            ValueError: if value does not represent a boolean
    """
    if value.lower() == 'true':
        return True
    elif value.lower() == 'false':
        return False
    else:
        raise ValueError('{} does not represent a boolean value.'.format(value))

# Luigi has a great decorator, inherits, which "copies parameters (and nothing else) from one task class to another,
# and avoids direct pythonic inheritance". This is great in that it avoids the difficulties of sharing common
# parameters between dependent tasks. Read more here:
# https://luigi.readthedocs.io/en/stable/api/luigi.util.html#using-inherits-and-requires-to-ease-parameter-pain.
# However, we use a version of Luigi that has an older implementation of this decorator, which creates a
# new wrapper class that subclasses the wrapped class. Normally, this isn't a problem. But because we're calling
# super() in the run method, we run into a problem. Let's say that the subclassed class has name A. When we call
# the decorator on A, we are returned a wrapper version of A, which is assigned the name A. Therefore, in the MRO
# of class A, A appears twice. When super() in called in the run method, A is returned, leading to infinite recursion.
# In future versions of Luigi, this is fixed. This is a temporary workaround.


CombineCourseEnrollments = inherits(ExportVerticaTableToS3Task)(CombineCourseEnrollmentsTask)
CombineCourseEnrollments.__name__ = 'CombineCourseEnrollments'
CombineCourseEnrollments.__class__._reg.append(CombineCourseEnrollments)

CountCourseEnrollments = inherits(CombineCourseEnrollments)(CountCourseEnrollmentsTask)
CountCourseEnrollments.__name__ = 'CountCourseEnrollments'
CountCourseEnrollments.__class__._reg.append(CountCourseEnrollments)

CountProgramCohortEnrollments = inherits(CountCourseEnrollments)(CountProgramCohortEnrollmentsTask)
CountProgramCohortEnrollments.__name__ = 'CountProgramCohortEnrollments'
CountProgramCohortEnrollments.__class__._reg.append(CountProgramCohortEnrollments)

BuildAggregateProgramReport = inherits(CountCourseEnrollments)(BuildAggregateProgramReportTask)
BuildAggregateProgramReport.__name__ = 'BuildAggregateProgramReport'
BuildAggregateProgramReport.__class__._reg.append(BuildAggregateProgramReport)
