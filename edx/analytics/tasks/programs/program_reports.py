import csv
import datetime
import json
import logging
from collections import namedtuple, OrderedDict

import luigi
from luigi.util import inherits

from edx.analytics.tasks.common.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask
from edx.analytics.tasks.common.vertica_export import VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, ExportVerticaTableToS3Task
from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class ProgramsReportTaskMixin(object):
    """
    Provides Luigi parameters common to the BuildLearnerProgramReport task and the BuildAggregateProgramReport task.
    Namely,
    - output_root: URL pointing to the location reports should be stored
    - overwrite: a boolean representing whether or not to overwrite the existing output for this task. Luigi tasks
        are idempotent, so given numerous same inputs, a task will generate the same output for each set of inputs.
        This may not be a desirable behavior when, say, the underlying data or the output of (an) upstream task(s) has
        changed and we need a fresh run of the task.
    - overwrite_export: a boolean representing whether or not to overwrite the Sqoop database export. As above, Luigi
        tasks are idempotent. This particular flag is used to indicate to the upstream.
        ExportVerticaTableToS3Task whether or not to re-export the Vertica table to S3
    - table_name: the table containing the underlying data, used by Sqoop in the upstream ExportVerticaTableToS3Task
    - sqoop_null_string: the string used to replace any null values encountered by Sqoop in the
        ExportVerticaTableToS3Task; we use "null" here because we want to display "null" in the output CSV anyway.
    - vertica_schema_name: the Vertica schema to run this task against
    """
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
    table_name = luigi.Parameter(
        default='learner_enrollments',
        description='Table containing enrollment rows to report on',
    )
    sqoop_null_string = luigi.Parameter(
        default='null',
        description='A string replacement value for any (null) values encountered by Sqoop when exporting from Vertica.',
    )
    vertica_credentials = luigi.Parameter(
        config_path={'section': 'vertica-export', 'name': 'credentials'},
        description='Path to the external Vertica access credentials file.',
    )
    vertica_schema_name = luigi.Parameter(
        default='programs_reporting',
        description='Vertica schema containing reporting table',
    )
    vertica_warehouse_name = luigi.Parameter(
        default='warehouse',
        description='The Vertica warehouse that houses the schema being copied.'
    )


class RemoveOutputMixin(object):

    def run(self):
        """
        Clear out output if overwrite requested.
        """
        self.remove_output_on_overwrite()
        super(RemoveOutputMixin, self).run()


class BuildLearnerProgramReport(OverwriteOutputMixin, ProgramsReportTaskMixin, RemoveOutputMixin, MultiOutputMapReduceJobTask):
    """
    Generates CSV reports on individual program enrollment.

    The task accepts the parameters inherited from the ProgramsReportTaskMixin.
    It also accepts the following parameters:
        - date: the date the report is run, which is used in the report CSV name to tag the report
        - report_name: the name of the report, which is used in the report CSV name
    """
    report_name = luigi.Parameter(
        default='learner_report',
        description='Name of report file(s) to output'
    )
    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date. Used to tag report date'
    )

    def __init__(self, *args, **kwargs):
        super(BuildLearnerProgramReport, self).__init__(*args, **kwargs)
        self.columns = self.get_column_names()

    def requires(self):
        return self.clone(ExportVerticaTableToS3Task, overwrite=(self.overwrite_export and self.overwrite))

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

    def mapper(self, line):
        """
        Group input by authoring institution and program
        """
        (org, _, program_uuid, _) = line.split(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii'), 3)
        yield (org, program_uuid), line

    def output_path_for_key(self, key):
        org_key, program_uuid = key
        filename = u'{}__{}.csv'.format(self.report_name, self.date)
        return url_path_join(self.output_root, org_key, program_uuid, filename)

    def multi_output_reducer(self, key, values, output_file):
        """
        Map export values to report output fields and write to csv.  Drops any extra columns
        """
        writer = csv.DictWriter(output_file, self.columns)
        writer.writerow(dict(
            (k, k) for k in self.columns
        ))

        for content in values:
            fields = content.split(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii'))
            row = {field_key: field_value for field_key, field_value in zip(self.columns, fields)}
            writer.writerow(row)


class CombineCourseEnrollmentsTask(OverwriteOutputMixin, ProgramsReportTaskMixin, RemoveOutputMixin, MapReduceJobTask):
    """
    A Map Reduce task that combines multiple course run enrollment records for a single course into a single
    record per course. It does some aggregation on data that may differ across multiple enrollments in the same course.

    The distinct enrollment tracks are joined together.
    The year of the earliest enrollment in any run of a course is used as the learner's entry year in that course.
    If there is no enrollment date information for all runs, the learner's entry year is null.
    The course is considered completed by the user if at least one run of the course is completed.

    The task accepts the parameters inherited from the ProgramsReportTaskMixin.
    """
    COMBINED_COURSE_ENROLL_FIELDS = [
        'authoring_org', 'program_title', 'program_uuid', 'program_type',
        'user_id', 'name', 'username', 'user_key', 'course_title', 'course_run_key', 'external_course_run_key',
        'track', 'grade', 'letter_grade', 'date_first_enrolled', 'date_last_unenrolled', 'current_enrollment_is_active',
        'first_verified_enrollment_time', 'course_run_completed', 'date_completed', 'program_completed', 'course_key', 'timestamp',
    ]
    CombinedCourseEnrollEntry = namedtuple('CombinedCourseEnrollEntry', COMBINED_COURSE_ENROLL_FIELDS)

    def requires(self):
        return self.clone(ExportVerticaTableToS3Task, overwrite=(self.overwrite_export and self.overwrite))

    def mapper(self, line):
        """Yield a (key, value) tuple for each course run enrollment record."""
        fields = line.split(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii'))
        entry = self.CombinedCourseEnrollEntry(*fields)

        yield (entry.authoring_org, entry.program_uuid, entry.user_id, entry.course_key, entry.timestamp), line

    def reducer(self, key, values):
        """
        For a given key, representing a leaner's enrollment in a particular course as part of a
        program at a particular time, do some aggregation on the values, which repesent course run
        enrollments in said course.

        In particular,
            - combine the different distinct enrollment tracks the learner has been enrolled in into a comma
              separated string
            - find the earliest enrollment time across all course run enrollments for that course and use the
              year as the learner's entry year
                - if all values are null, indicating missing data, use null as the entry year
            - determine whether the learner completed the course by checking whether the learner has completed at least
              one run of the course

        Yield a (key, value) pair where the key is empty, and the value represents information
        about a learner's enrollment in a course.
        """
        entry_years = set()
        completed = False
        program_completed = False
        num_course_run_enrollments = 0
        tracks = set()

        authoring_org, program_uuid, user_id, course_key, timestamp = key

        for value in values:
            num_course_run_enrollments += 1

            fields = value.split(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii'))
            entry = self.CombinedCourseEnrollEntry(*fields)

            program_completed = string_to_bool(entry.program_completed)

            track = entry.track
            if track == 'no-id-professional':
                # for the purposes of this report, treat "no-id-professional"
                # and "professional" as the same track
                track = 'professional'
            tracks.add(track)

            is_course_run_completed = string_to_bool(entry.course_run_completed)
            completed = completed or is_course_run_completed

            if entry.date_first_enrolled != self.sqoop_null_string:
                year = datetime.datetime.strptime(entry.date_first_enrolled, '%Y-%m-%d %H:%M:%S.%f').year
                entry_years.add(year)

        # the method that writes the output of the reducers to a file writes the
        # elements of the output as a tab separated string,
        # so to keep the list of tracks together, use a comma separated string
        tracks = ','.join(tracks)

        # if all course run enrollments have null for their first enrollment time, use sqoop_null_string
        # find the minimum value for the entry year excluding None; if None is the only value,
        # then entry year is the value of sqoop_null_string
        if len(entry_years) > 0:
            entry_year = min(entry_years)
        else:
            entry_year = self.sqoop_null_string

        yield [authoring_org, program_uuid,
               user_id, tracks, num_course_run_enrollments, entry_year,
               completed, program_completed, timestamp]

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

    It also includes
        - whether the learner has completed the program
        - the earliest year a learner has enrolled in any course within the program as that learner's entry year

    The task accepts the same parameters as the CombineCourseEnrollmentsTask, from which it inherits parameters.
    """

    COUNT_COURSE_ENROLLMENT_FIELDS = [
        'authoring_org', 'program_uuid',
        'user_id', 'tracks', 'num_course_run_enrollments', 'entry_year',
        'course_completed', 'program_completed', 'timestamp',
    ]
    CountCourseEnrollmentsEntry = namedtuple('CountCourseEnrollmentsEntry', COUNT_COURSE_ENROLLMENT_FIELDS)

    def requires(self):
        return self.clone(CombineCourseEnrollmentsTask)

    def mapper(self, line):
        """Yield a (key, value) tuple for each learner enrolled in a program."""

        # although we originally split on the sqoop_fields_terminated_by parameter,
        # the writer of the previous reduce task uses a tab delimiter
        fields = line.split('\t')
        entry = self.CountCourseEnrollmentsEntry(*fields)

        yield (entry.authoring_org, entry.program_uuid, entry.user_id, entry.timestamp), line

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
        authoring_org, program_uuid, user_id, timestamp = key

        entry_years = set()
        is_program_completed = False
        num_course_run_enrollments = 0
        num_completed_courses = 0
        num_audit_enrollments = 0
        num_verified_enrollments = 0
        num_professional_enrollments = 0
        num_masters_enrollments = 0

        for value in values:
            # although we originally split on the sqoop_fields_terminated_by parameter,
            # the writer of the previous reduce task uses a tab delimiter
            fields = value.split('\t')

            entry = self.CountCourseEnrollmentsEntry(*fields)

            num_course_run_enrollments += int(entry.num_course_run_enrollments)

            is_course_completed = string_to_bool(entry.course_completed)
            if is_course_completed:
                num_completed_courses += 1

            is_program_completed = is_program_completed or string_to_bool(entry.program_completed)

            tracks = entry.tracks.split(',')
            for track in tracks:
                if track == 'audit':
                    num_audit_enrollments += 1
                elif track == 'verified':
                    num_verified_enrollments += 1
                elif track == 'professional' or track == 'no-id-professional':
                    num_professional_enrollments += 1
                elif track == 'masters':
                    num_masters_enrollments += 1

            if entry.entry_year != self.sqoop_null_string:
                entry_years.add(entry.entry_year)

        # find the learner's entry year across all courses they have enrolled in as part of a program
        # find the minimum value for the entry year excluding sqoop_null_string; if sqoop_null_string is the only value,
        # then entry year is sqoop_null_string
        if len(entry_years) > 0:
            entry_year = min(entry_years)
        else:
            entry_year = self.sqoop_null_string

        yield [authoring_org, program_uuid, user_id,
               entry_year, num_course_run_enrollments, num_completed_courses,
               num_audit_enrollments, num_verified_enrollments,
               num_professional_enrollments, num_masters_enrollments,
               is_program_completed, timestamp]

    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp', 'CountCourseEnrollments/'))


# See comment at the end of this file regarding BuildAggregateProgramReport.
CountCourseEnrollments = inherits(CombineCourseEnrollmentsTask)(CountCourseEnrollmentsTask)
CountCourseEnrollments.__name__ = 'CountCourseEnrollments'
CountCourseEnrollments.__class__._reg.append(CountCourseEnrollments)


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

    The task accepts the same parameters as the CountCourseEnrollments, from which it inherits parameters
    """

    COUNT_PROGRAM_ENROLLMENTS_FIELDS = [
        'authoring_org', 'program_uuid',
        'user_id', 'entry_year', 'num_course_run_enrollments', 'num_completed_courses',
        'num_audit_enrollments', 'num_verified_enrollments', 'num_professional_enrollments',
        'num_masters_enrollments', 'is_program_completed', 'timestamp',
    ]
    CountProgramEnrollmentsEntry = namedtuple('CountProgramEnrollmentsEntry', COUNT_PROGRAM_ENROLLMENTS_FIELDS)

    @staticmethod
    def aggregate_enrollment_totals(counts, number_of_enrollments):
        """
        Helper function to build up a dictionary mapping a count of enrollments in
        a specific number of courses
        """
        for n in range(number_of_enrollments):
                if n in counts.keys():
                    counts[n] += 1
                else:
                    counts[n] = 1

    def requires(self):
        return self.clone(CountCourseEnrollments)

    def mapper(self, line):
        """Yield a (key, value) tuple for each program."""
        fields = line.split('\t')
        entry = self.CountProgramEnrollmentsEntry(*fields)

        yield (entry.authoring_org, entry.program_uuid, entry.entry_year, entry.timestamp), line

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
        authoring_org, program_uuid, entry_year, timestamp = key

        total_num_learners = 0
        total_num_run_enrollments = 0
        total_num_program_completions = 0

        cnt_learners_in_audit = {}
        cnt_learners_in_verified = {}
        cnt_learners_in_professional = {}
        cnt_learners_in_masters = {}
        cnt_learners_in_completed_courses = {}

        for value in values:
            fields = value.split('\t')
            entry = self.CountProgramEnrollmentsEntry(*fields)

            total_num_learners += 1
            total_num_run_enrollments += int(entry.num_course_run_enrollments)

            is_program_completed = entry.is_program_completed

            if string_to_bool(is_program_completed):
                total_num_program_completions += 1

            self.aggregate_enrollment_totals(cnt_learners_in_audit, int(entry.num_audit_enrollments))
            self.aggregate_enrollment_totals(cnt_learners_in_verified, int(entry.num_verified_enrollments))
            self.aggregate_enrollment_totals(cnt_learners_in_professional, int(entry.num_professional_enrollments))
            self.aggregate_enrollment_totals(cnt_learners_in_masters, int(entry.num_masters_enrollments))
            self.aggregate_enrollment_totals(cnt_learners_in_completed_courses, int(entry.num_completed_courses))

            num_audit_enrollments = int(entry.num_audit_enrollments)
            num_verified_enrollments = int(entry.num_verified_enrollments)
            num_professional_enrollments = int(entry.num_professional_enrollments)
            num_masters_enrollments = int(entry.num_masters_enrollments)

        result = OrderedDict([
            ('authoring_org', authoring_org),
            ('program_uuid', program_uuid),
            ('entry_year', entry_year),
            ('total_learners', total_num_learners),
            ('total_enrollments', total_num_run_enrollments),
            ('total_completions', total_num_program_completions),
            ('audit_enrollment_counts', cnt_learners_in_audit.values()),
            ('verified_enrollment_counts', cnt_learners_in_verified.values()),
            ('professional_enrollment_counts', cnt_learners_in_professional.values()),
            ('masters_enrollment_counts', cnt_learners_in_masters.values()),
            ('course_completion_counts', cnt_learners_in_completed_courses.values()),
            ('timestamp', timestamp),
        ])

        yield (json.dumps(result),)


    def output(self):
        return get_target_from_url(url_path_join(self.output_root, 'temp/CountProgramCohortEnrollments/'))


# See comment at the end of this file regarding BuildAggregateProgramReport.
CountProgramCohortEnrollments = inherits(CountCourseEnrollments)(CountProgramCohortEnrollmentsTask)
CountProgramCohortEnrollments.__name__ = 'CountProgramCohortEnrollments'
CountProgramCohortEnrollments.__class__._reg.append(CountProgramCohortEnrollments)


class BuildAggregateProgramReportTask(OverwriteOutputMixin, RemoveOutputMixin, MultiOutputMapReduceJobTask):
    """
    A Map Reduce task that writes a program's aggregate enrollment data to organization-program specific file.

    The task accepts the same parameters as the CountProgramCohortEnrollments, from which it inherits parameters,
    as well as the following parameters:
            - date: the date the report is run, which is used in the report CSV name to tag the report
            - report_name: the name of the report, which is used in the report CSV name
    """

    date = luigi.DateParameter(
        default=datetime.datetime.utcnow().date(),
        description='Current run date. Used to tag report date'
    )
    report_name = luigi.Parameter(
        default='aggregate_report'
    )
    programs_table = luigi.Parameter(
        default='program_metadata'
    )

    PROGRAM_METADATA_FIELDS = [
        'authoring_org', 'program_title', 'program_uuid', 'program_type', 'course_count'
    ]
    ProgramMetadataEntry = namedtuple('ProgramMetadataEntry', PROGRAM_METADATA_FIELDS)

    def __init__(self, *args, **kwargs):
        super(BuildAggregateProgramReportTask, self).__init__(*args, **kwargs)

    def requires(self):
        yield self.clone(
            ExportVerticaTableToS3Task,
            table_name=self.programs_table,
            overwrite=(self.overwrite_export and self.overwrite)
        )
        yield self.clone(CountProgramCohortEnrollments)

    @staticmethod
    def get_column_names(num_courses):
        """
        List names of columns as they should appear in the CSV.
        """

        columns = [
            'Authoring Institution',
            'Program Title',
            'Program UUID',
            'Program Type',
            'Entry Year',
            'Total Learners',
            'Total Number Enrollments',
        ]
        columns.append('Total Number of Program Completions')
        columns.extend(['Number of Learners in {}+ Audit'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners in {}+ Verified'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners in {}+ Professional'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners in {}+ Master\'s'.format(num) for num in range(1, num_courses + 1)])
        columns.extend(['Number of Learners Completed {}+ Courses'.format(num) for num in range(1, num_courses + 1)])
        columns.append('Timestamp')
        return columns

    def output_path_for_key(self, key):
        authoring_institution, program_uuid = key
        filename = u'{}__{}.csv'.format(self.report_name, self.date)
        return url_path_join(self.output_root, authoring_institution, program_uuid, filename)

    def mapper(self, line):
        """
        Merges cohort enrollment output and a matching program entry from the
        program metadata table into a common data structure.
        """
        input_type = 'cohort_enrollments'
        export_delimiter = VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii')
        if export_delimiter in line:
            authoring_institution, _, program_uuid, _ = line.split(export_delimiter, 3)
            input_type = 'program_metadata'
        else:
            cohort = json.loads(line)
            authoring_institution = cohort['authoring_org'].encode('ascii')
            program_uuid = cohort['program_uuid'].encode('ascii')

        value = {
            'input_type': input_type,
            'data': line,
        }
        yield (authoring_institution, program_uuid), value

    def multi_output_reducer(self, key, values, output_file):
        cohorts_by_year = {}
        program = None
        for value in values:
            input_type = value['input_type']

            if input_type == 'program_metadata':
                fields = value['data'].split(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.encode('ascii'))
                program = self.ProgramMetadataEntry(*fields)
            else:
                cohort = json.loads(value['data'])
                entry_year = cohort['entry_year']
                cohorts_by_year[entry_year] = cohort

        if not program:
            raise Exception(
                'Cannot write report for {} program {}. No matching program_metadata entry found.'.format(
                    key[0],
                    key[1],
                )
            )

        program_course_count = int(program.course_count)
        columns = self.get_column_names(program_course_count)
        writer = csv.DictWriter(output_file, columns) 
        writer.writeheader()

        for entry_year, cohort in sorted(cohorts_by_year.items(), key=lambda x:x[1]):

            def append_counts(row, values):
                for n in range(program_course_count):
                    if len(values):
                        row.append(values.pop(0))
                    else:
                        row.append(0)

            row_items = [
                program.authoring_org,
                program.program_title,
                program.program_uuid,
                program.program_type,
                cohort.get('entry_year'),
                cohort.get('total_learners'),
                cohort.get('total_enrollments'),
                cohort.get('total_completions'),
            ]
            append_counts(row_items, cohort.get('audit_enrollment_counts'))
            append_counts(row_items, cohort.get('verified_enrollment_counts'))
            append_counts(row_items, cohort.get('professional_enrollment_counts'))
            append_counts(row_items, cohort.get('masters_enrollment_counts'))
            append_counts(row_items, cohort.get('course_completion_counts'))
            
            row_items.append(cohort.get('timestamp'))

            writer.writerow({field_key: field_value for field_key, field_value in zip(columns, row_items)})


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
# Note that we do not use this decorator with any task that is directly downstream of ExportVerticaTableToS3Task
# due to issues with the "inheritance" of the sqoop_fields_terminated_by parameter, whose default value is
# u'\x01'; jobs running on EMR fail fails with the following error:
# "[Fatal Error] job.xml:209:161: Character reference "&#1" is an invalid XML character."
BuildAggregateProgramReport = inherits(CountCourseEnrollments)(BuildAggregateProgramReportTask)
BuildAggregateProgramReport.__name__ = 'BuildAggregateProgramReport'
BuildAggregateProgramReport.__class__._reg.append(BuildAggregateProgramReport)
