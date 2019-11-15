import csv
import datetime
import logging

import luigi
from luigi.util import inherits

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.vertica_export import ExportVerticaTableToS3Task, get_vertica_table_schema, VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER
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


class RemoveOutputMixin(object):

    def run(self):
        """
        Clear out output if overwrite requested.
        """
        self.remove_output_on_overwrite()
        super(RemoveOutputMixin, self).run()


class BaseProgramReportsTask(OverwriteOutputMixin, ProgramsReportTaskMixin, RemoveOutputMixin, MultiOutputMapReduceJobTask):
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
        raise NotImplementedError

    def multi_output_reducer(self, key, values, output_file):
        raise NotImplementedError

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


class CombineCourseEnrollmentsTask(OverwriteOutputMixin, RemoveOutputMixin, MapReduceJobTask):
    """
        A Map Reduce task that combines multiple course run enrollment records for a single course into a single
        record per course. It does some aggregation on data that may differ across multiple enrollments in the same course.

        The distinct enrollment tracks are joined together.
        The year of the earliest enrollment in any run of a course is used as the learner's entry year in that course. If there is no
        enrollment date information for all runs, the learner's entry year is null.
        The course is considered completed by the user if at least one run of the course is completed.
    """
    AUTHORING_ORG_INDEX = 0
    PROGRAM_TITLE_INDEX = 1
    PROGRAM_UUID_INDEX = 2
    PROGRAM_TYPE_INDEX = 3
    USER_ID_INDEX = 4
    TRACK_INDEX = 11
    ENTRY_YEAR_INDEX = 14
    COURSE_RUN_COMPLETED_INDEX = 18
    PROGRAM_COMPLETED_INDEX = 20
    COURSE_KEY_INDEX = 21
    TIMESTAMP_INDEX = 22

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
    sqoop_fields_terminated_by = luigi.Parameter(
        default=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER,
        description='The field delimiter used by Sqoop.'
    )

    vertica_credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external access credentials file.',
    )
    table_name = luigi.Parameter(
        default='learner_enrollments',
        description='Table containing enrollment rows to report on',
    )
    vertica_warehouse_name = luigi.Parameter(
        default='docker',
        description='The Vertica warehouse that houses the report schema.',
    )
    sqoop_delimiter_replacement = luigi.Parameter(
        default=' ',
        description='The string replacement value for special characters encountered by Sqoop when exporting from '
                    'Vertica.',
    )

    def requires(self):
        # return self.clone(ExportVerticaTableToS3Task, overwrite=(self.overwrite_export and self.overwrite))
        return ExportVerticaTableToS3Task(
            vertica_schema_name=self.vertica_schema_name,
            sqoop_null_string=self.sqoop_null_string,
            sqoop_fields_terminated_by=self.sqoop_fields_terminated_by,
            overwrite=self.overwrite_export and self.overwrite,
            vertica_credentials=self.vertica_credentials,
            vertica_warehouse_name=self.vertica_warehouse_name,
            table_name=self.table_name,
        )

    def mapper(self, line):
        """Yield a (key, value) tuple for each course run enrollment record."""
        fields = line.split(self.sqoop_fields_terminated_by.encode('ascii'))

        authoring_org = fields[self.AUTHORING_ORG_INDEX]
        program_title = fields[self.PROGRAM_TITLE_INDEX]
        program_uuid = fields[self.PROGRAM_UUID_INDEX]
        program_type = fields[self.PROGRAM_TYPE_INDEX]
        user_id = fields[self.USER_ID_INDEX]
        course_key = fields[self.COURSE_KEY_INDEX]
        timestamp = fields[self.TIMESTAMP_INDEX]

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
        num_course_run_enrollments = 0
        tracks = set()

        authoring_org, program_type, program_uuid, program_title, user_id, course_key, timestamp = key

        for value in values:
            num_course_run_enrollments += 1

            fields = value.split(self.sqoop_fields_terminated_by.encode('ascii'))

            program_completed = string_to_bool(fields[self.PROGRAM_COMPLETED_INDEX])

            track = fields[self.TRACK_INDEX]
            tracks.add(track)

            is_course_run_completed = string_to_bool(fields[self.COURSE_RUN_COMPLETED_INDEX])
            completed = completed or is_course_run_completed

            if fields[self.ENTRY_YEAR_INDEX] != self.sqoop_null_string:
                year = datetime.datetime.strptime(fields[self.ENTRY_YEAR_INDEX], '%Y-%m-%d %H:%M:%S.%f').year
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

        yield [authoring_org, program_type, program_title, program_uuid, user_id, tracks, num_course_run_enrollments, entry_year, completed, program_completed, timestamp]

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp', 'CombineCourseEnrollments/'))

# CombineCourseEnrollments = inherits(ExportVerticaTableToS3Task)(CombineCourseEnrollmentsTask)
# CombineCourseEnrollments.__name__ = 'CombineCourseEnrollments'
# CombineCourseEnrollments.__class__._reg.append(CombineCourseEnrollments)

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

        It also includes
            - whether the learner has completed the program
            - the earliest year a learner has enrolled in any course within the program as that learner's entry year
    """

    AUTHORING_ORG_INDEX = 0
    PROGRAM_TYPE_INDEX = 1
    PROGRAM_TITLE_INDEX = 2
    PROGRAM_UUID_INDEX = 3
    USER_ID_INDEX = 4
    TRACKS_INDEX = 5
    NUM_COURSE_RUN_ENROLLMENTS_INDEX = 6
    ENTRY_YEAR_INDEX = 7
    COURSE_COMPLETED_INDEX = 8
    PROGRAM_COMPLETED_INDEX = 9
    TIMESTAMP_INDEX = 10

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

    sqoop_fields_terminated_by = luigi.Parameter(
        default=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER,
        description='The field delimiter used by Sqoop.'
    )

    def requires(self):
        # return self.clone(CombineCourseEnrollments)
        return self.clone(CombineCourseEnrollmentsTask)

    def mapper(self, line):
        """Yield a (key, value) tuple for each learner enrolled in a program."""

        # although we originally split on the sqoop_fields_terminated_by parameter,
        # the writer of the previous reduce task uses a tab delimeter
        fields = line.split('\t')

        authoring_org = fields[self.AUTHORING_ORG_INDEX]
        program_type = fields[self.PROGRAM_TYPE_INDEX]
        program_title = fields[self.PROGRAM_TITLE_INDEX]
        program_uuid = fields[self.PROGRAM_UUID_INDEX]
        user_id = fields[self.USER_ID_INDEX]
        timestamp = fields[self.TIMESTAMP_INDEX]

        yield (authoring_org, program_type, program_title, program_uuid, user_id, timestamp), line

    def reducer(self, key, values):
        """
            For a given key, representing a learner enrolled in a program,
            do some aggregation on the values, which repesent course enrollments in said program.

            In particular, count
                - the total number of course enrollments
                - the number of completed courses
                - the number of courses in which the learner is enrolled in the following tracks
                    - audit
                    - verified
                    - professional or no-id-professional
                    - masters

            Also include
                - whether the learner has completed the program
                - the earliest year a learner has enrolled in any course within the program as that learner's entry year
        """
        authoring_org, program_type, program_title, program_uuid, user_id, timestamp = key

        entry_years = set()
        is_program_completed = False
        num_course_run_enrollments = 0
        num_completed_courses = 0
        num_audit_enrollments = 0
        num_verified_enrollments = 0
        num_professional_enrollments = 0
        num_masters_enrollents = 0

        for value in values:
            # although we originally split on the sqoop_fields_terminated_by parameter,
            # the writer of the previous reduce task uses a tab delimeter
            fields = value.split('\t')

            num_course_run_enrollments += int(fields[self.NUM_COURSE_RUN_ENROLLMENTS_INDEX])

            is_course_completed = string_to_bool(fields[self.COURSE_COMPLETED_INDEX])
            if is_course_completed:
                num_completed_courses += 1

            is_program_completed = is_program_completed or string_to_bool(fields[self.PROGRAM_COMPLETED_INDEX])

            tracks = fields[self.TRACKS_INDEX].split(',')
            for track in tracks:
                if track == 'audit':
                    num_audit_enrollments += 1
                elif track == 'verified':
                    num_verified_enrollments += 1
                elif track == 'professional' or track == 'no-id-professional':
                    num_professional_enrollments += 1
                elif track == 'masters':
                    num_masters_enrollents += 1

            if fields[self.ENTRY_YEAR_INDEX] != 'null':
                entry_years.add(fields[self.ENTRY_YEAR_INDEX])

        # find the learner's entry year across all courses they have enrolled in as part of a program
        # find the minimum value for the entry year excluding null; if null is the only value,
        # then entry year is null
        if len(entry_years) > 0:
            entry_year = min(entry_years)
        else:
            entry_year = 'null'

        yield [authoring_org, program_type, program_title, program_uuid, user_id, entry_year, num_course_run_enrollments, num_completed_courses, num_audit_enrollments, num_verified_enrollments, num_professional_enrollments, num_masters_enrollents, is_program_completed, timestamp]

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp', 'CountCourseEnrollments/'))

# CountCourseEnrollments = inherits(CombineCourseEnrollments)(CountCourseEnrollmentsTask)
# CountCourseEnrollments.__name__ = 'CountCourseEnrollments'
# CountCourseEnrollments.__class__._reg.append(CountCourseEnrollments)

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
    AUTHORING_ORG_INDEX = 0
    PROGRAM_TYPE_INDEX = 1
    PROGRAM_TITLE_INDEX = 2
    PROGRAM_UUID_INDEX = 3
    USER_ID_INDEX = 4
    ENTRY_YEAR_INDEX = 5
    NUM_COURSE_RUN_ENROLLMENTS_INDEX = 6
    NUM_COMPLETED_COURSES_INDEX = 7
    NUM_AUDIT_ENROLLMENTS_INDEX = 8
    NUM_VERIFIED_ENROLLMENTS_INDEX = 9
    NUM_PROFESSIONAL_ENROLLMENTS_INDEX = 10
    NUM_MASTERS_ENROLLMENTS_INDEX = 11
    PROGRAM_COMPLETED_INDEX = 12
    TIMESTAMP_INDEX = 13

    def requires(self):
        return self.clone(CountCourseEnrollments)

    def mapper(self, line):
        """Yield a (key, value) tuple for each program."""
        fields = line.split('\t')
        authoring_org = fields[self.AUTHORING_ORG_INDEX]
        program_type = fields[self.PROGRAM_TYPE_INDEX]
        program_title = fields[self.PROGRAM_TITLE_INDEX]
        program_uuid = fields[self.PROGRAM_UUID_INDEX]
        entry_year = fields[self.ENTRY_YEAR_INDEX]
        timestamp = fields[self.TIMESTAMP_INDEX]

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
        total_num_course_run_enrollments = 0
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
            total_num_course_run_enrollments += int(fields[self.NUM_COURSE_RUN_ENROLLMENTS_INDEX])

            is_program_completed = fields[self.PROGRAM_COMPLETED_INDEX]
            if string_to_bool(is_program_completed):
                total_num_program_completions += 1

            num_audit_enrollments = int(fields[self.NUM_AUDIT_ENROLLMENTS_INDEX])
            num_verified_enrollments = int(fields[self.NUM_VERIFIED_ENROLLMENTS_INDEX])
            num_professional_enrollments = int(fields[self.NUM_PROFESSIONAL_ENROLLMENTS_INDEX])
            num_masters_enrollments = int(fields[self.NUM_MASTERS_ENROLLMENTS_INDEX])

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

        row = [authoring_org, program_type, program_title, program_uuid, entry_year, total_num_learners, total_num_course_run_enrollments]
        for num in num_learners_in_audit + num_learners_in_verified + num_learners_in_professional + num_learners_in_masters + num_learners_completed_courses:
            row.append(num)

        row.append(total_num_program_completions)
        row.append(timestamp)

        yield row

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp/CountProgramCohortEnrollments/'))

# CountProgramCohortEnrollments = inherits(CountCourseEnrollments)(CountProgramCohortEnrollmentsTask)
# CountProgramCohortEnrollments.__name__ = 'CountProgramCohortEnrollments'
# CountProgramCohortEnrollments.__class__._reg.append(CountProgramCohortEnrollments)

class BuildAggregateProgramReportTask(OverwriteOutputMixin, RemoveOutputMixin, MultiOutputMapReduceJobTask):
    """A Map Reduce task that writes a program's aggregate enrollment data to organization-program specific file."""

    report_name = luigi.Parameter(
        default='aggregate_report'
    )

    def __init__(self, *args, **kwargs):
        super(BuildAggregateProgramReportTask, self).__init__(*args, **kwargs)
        self.columns = self.get_column_names()

    def requires(self):
        return self.clone(CountProgramCohortEnrollments)

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
        columns.extend(['Number of Learners in {}+ Audit'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners in {}+ Verified'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners in {}+ Professional'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners in {}+ Master\'s'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners Completed {}+ Courses'.format(num) for num in range(1, num_courses + 1)])
        columns.append('Total Number of Program Completions')
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


# CombineCourseEnrollments = inherits(ExportVerticaTableToS3Task)(CombineCourseEnrollmentsTask)
# CombineCourseEnrollments.__name__ = 'CombineCourseEnrollments'
# CombineCourseEnrollments.__class__._reg.append(CombineCourseEnrollments)

# CountCourseEnrollments = inherits(CombineCourseEnrollments)(CountCourseEnrollmentsTask)
# CountCourseEnrollments.__name__ = 'CountCourseEnrollments'
# CountCourseEnrollments.__class__._reg.append(CountCourseEnrollments)
# 
# CountProgramCohortEnrollments = inherits(CountCourseEnrollments)(CountProgramCohortEnrollmentsTask)
# CountProgramCohortEnrollments.__name__ = 'CountProgramCohortEnrollments'
# CountProgramCohortEnrollments.__class__._reg.append(CountProgramCohortEnrollments)

# BuildAggregateProgramReport = inherits(CountCourseEnrollments)(BuildAggregateProgramReportTask)
# BuildAggregateProgramReport.__name__ = 'BuildAggregateProgramReport'
# BuildAggregateProgramReport.__class__._reg.append(BuildAggregateProgramReport)
