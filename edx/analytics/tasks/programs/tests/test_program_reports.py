import json
import os
import shutil
import tempfile
from collections import OrderedDict
from datetime import datetime
from string import join
from unittest import TestCase

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.common.vertica_export import VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER
from edx.analytics.tasks.programs.program_reports import (
    BuildAggregateProgramReport, BuildLearnerProgramReport, CombineCourseEnrollmentsTask, CountCourseEnrollments,
    CountProgramCohortEnrollments
)


class ProgramReportTestMixin(object):

    org_key = 'UoX'
    program_type = 'masters'
    program_title = 'Test Program'
    program_uuid = '77c98259-de10-4c55-9b89-6b45320a25ba'
    course_count = 6

    def setup_dirs(self):
        """Create temp input and output dirs."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_rootdir, 'output')
        os.mkdir(self.output_dir)
        self.input_dir = os.path.join(self.temp_rootdir, 'input')
        os.mkdir(self.input_dir)
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    def create_enrollment_input(self, org_key, program_uuid, *args, **kwargs):
        """Create input row matching sqoop output on the learner_enrollments table"""
        learner_enrollment_template = OrderedDict([
            ('authoring_institution', org_key),
            ('program_title', self.program_title),
            ('program_uuid', program_uuid),
            ('program_type', self.program_type),
            ('user_id', 10),
            ('name', 'User'),
            ('username', 'test_user'),
            ('user_key', None),
            ('course_title', 'Test Course'),
            ('course_run_key', 'course-v1:edX+UoX+Test_Course+2T2019'),
            ('external_course_run_key', None),
            ('track', 'masters'),
            ('grade', None),
            ('letter_grade', None),
            ('date_first_enrolled', None),
            ('date_last_unenrolled', None),
            ('current_enrollment_is_active', 'False'),
            ('first_verified_enrollment_time', None),
            ('completed', 'False'),
            ('date_completed', None),
            ('completed_program', 'False'),
            ('course_key', 'course-v1:edX+UoX+Test_Course'),
            ('timestamp', None)
        ])
        for key, value in kwargs.items():
            learner_enrollment_template[key] = value
        return self.create_sqoop_export_row(learner_enrollment_template)

    def create_program_input(self, org_key, program_uuid):
        """Create input row matching sqoop output on the program_metadata table"""
        program_metadata_template = OrderedDict([
            ('authoring_institution', org_key),
            ('program_title', self.program_title),
            ('program_uuid', program_uuid),
            ('program_type', self.program_type),
            ('course_count', str(self.course_count)),
        ])
        return self.create_sqoop_export_row(program_metadata_template)

    def create_sqoop_export_row(self, data):
        """Convert a dictionary of fields into a sqoop export string"""
        export_row = ''
        sqoop_null_string = self.task.sqoop_null_string
        for field, value in data.items():
            if value is None:
                value = sqoop_null_string
            export_row += str(value) + VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER
        return export_row.rstrip(VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER)


class ProgramMultiOutputMapReduceReducerTestMixin:

    def create_expected_output(self, inputs, delimiter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER):
        """Returns list of lines expected in an output file"""
        columns = self.task_class.get_column_names()

        # first line is header
        header = ','.join(columns)
        expected = [header]
        for input_row in inputs:
            fields = input_row.split(delimiter)[:len(columns)]
            expected.append(','.join(fields))
        return expected

    def validate_output_files(self, expected, filename_suffix):
        """Ensure each output file contains the correct data"""
        for program_key in expected.keys():
            filename = os.path.join(self.output_dir, program_key[0], program_key[1], filename_suffix)
            with open(filename) as report_file:
                num_lines = 0
                for line in report_file:
                    self.assertEquals(line.rstrip(), expected[program_key][num_lines])
                    num_lines += 1
                self.assertEquals(num_lines, len(expected[program_key]))


class CohortEnrollmentCountTestMixin:

    @staticmethod
    def build_cohort_enrollment_output(
        authoring_org,
        program_uuid,
        *args,
        **kwargs
    ):
        out = OrderedDict([
            ('authoring_org', authoring_org),
            ('program_uuid', program_uuid),
            ('entry_year', '2019'),
            ('total_learners', 6),
            ('total_enrollments', 27),
            ('total_completions', 2),
            ('audit_enrollment_counts', [3,3]),
            ('verified_enrollment_counts', [1,1,1,1,1]),
            ('professional_enrollment_counts', []),
            ('masters_enrollment_counts', [6,4,4,1,1]),
            ('course_completion_counts', [6,4,3,2,2]),
            ('timestamp', 'null'),
        ])
        for key, value in kwargs.items():
            out[key] = value
        return json.dumps(out)


class LearnerProgramReportMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = BuildLearnerProgramReport

    def setUp(self):
        super(LearnerProgramReportMapperTest, self).setUp()

    def test_map_by_program(self):
        line = self.create_enrollment_input(self.org_key, self.program_uuid)
        self.assert_single_map_output(line, (self.org_key, self.program_uuid), line)


class LearnerProgramReportReducerTest(ProgramReportTestMixin, ReducerTestMixin, ProgramMultiOutputMapReduceReducerTestMixin, TestCase):

    task_class = BuildLearnerProgramReport
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
        )
        super(LearnerProgramReportReducerTest, self).setUp()

    def test_multi_file_reduce(self):
        programs = [
            (self.org_key, 'e86dc8e3-47cd-4875-87bb-98356e1aa876'),
            (self.org_key, '01015fd3-3b19-48b5-867c-1c78ef43a315')
        ]

        expected_results = {}
        for program_key in programs:
            org_key, program_uuid = program_key
            self.reduce_key = program_key
            inputs = [
                self.create_enrollment_input(org_key, program_uuid, user_id=1),
                self.create_enrollment_input(org_key, program_uuid, user_id=2)
            ]
            # MultiOutputMapReduceJobTasks.reduce() returns an empty tuple
            reducer_output = self._get_reducer_output(inputs)
            self.assertEquals(reducer_output, tuple())

            expected_results[program_key] = self.create_expected_output(inputs)

        self.validate_output_files(expected_results, 'learner_report__{}.csv'.format(self.DATE))


class CombineCourseEnrollmentsTaskMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = CombineCourseEnrollmentsTask
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )

        super(CombineCourseEnrollmentsTaskMapperTest, self).setUp()

    def test_mapper(self):
        line = self.create_enrollment_input(self.org_key, self.program_uuid)
        self.assert_single_map_output(line, (self.org_key, self.program_uuid, '10', 'course-v1:edX+UoX+Test_Course', self.task.sqoop_null_string), line)


class CombineCourseEnrollmentsTaskReducerTest(ProgramReportTestMixin, ReducerTestMixin, TestCase):

    task_class = CombineCourseEnrollmentsTask
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            table_name='table',
        )

        super(CombineCourseEnrollmentsTaskReducerTest, self).setUp()

    def test_reducer_multiple_tracks(self):
        self.reduce_key = (self.org_key, self.program_uuid, '10', 'course-v1:edX+UoX+Test_Course', self.task.sqoop_null_string)

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid),
            self.create_enrollment_input(self.org_key, self.program_uuid, track='verified'),
            self.create_enrollment_input(self.org_key, self.program_uuid, track='audit')
        ]
        self._check_output_complete_tuple(input, ([self.org_key, self.program_uuid, '10', 'audit,masters,verified', 3, 'null', False, False, self.task.sqoop_null_string],))

    def test_reducer_entry_year(self):
        self.reduce_key = (self.org_key, self.program_uuid, '10', 'course-v1:edX+UoX+Test_Course', self.task.sqoop_null_string)

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid, date_first_enrolled=str(datetime(2019, 12, 31, 10, 30, 15, 5))),
            self.create_enrollment_input(self.org_key, self.program_uuid, date_first_enrolled=str(datetime(2018, 12, 31, 10, 30, 15, 5)))
        ]
        self._check_output_complete_tuple(input, ([self.org_key, self.program_uuid, '10', 'masters', 2, 2018, False, False, self.task.sqoop_null_string],))

    def test_reducer_entry_year_null_enrollment_time(self):
        self.reduce_key = (self.org_key, self.program_uuid, '10', 'course-v1:edX+UoX+Test_Course', self.task.sqoop_null_string)

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid),
        ]

        self._check_output_complete_tuple(input, ([self.org_key, self.program_uuid, '10', 'masters', 1, 'null', False, False, self.task.sqoop_null_string],))

    def test_reducer_course_completed(self):
        self.reduce_key = (self.org_key, self.program_uuid, '10', 'course-v1:edX+UoX+Test_Course', self.task.sqoop_null_string)

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid),
            self.create_enrollment_input(self.org_key, self.program_uuid, completed=True)
        ]
        self._check_output_complete_tuple(input, ([self.org_key, self.program_uuid, '10', 'masters', 2, 'null', True, False, self.task.sqoop_null_string],))


class CountCourseEnrollmentsMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = CountCourseEnrollments
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )

        super(CountCourseEnrollmentsMapperTest, self).setUp()

    def test_mapper(self):
        line = '\t'.join([self.org_key, self.program_uuid, '10', 'audit,verified', '2', '2018', 'True', 'True', 'null'])
        self.assert_single_map_output(line, (self.org_key, self.program_uuid, '10', 'null'), line)


class CountCourseEnrollmentReducerTest(ProgramReportTestMixin, ReducerTestMixin, TestCase):

    task_class = CountCourseEnrollments
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
            table_name='table',
        )

        super(CountCourseEnrollmentReducerTest, self).setUp()

    def test_reducer(self):
        self.reduce_key = (self.org_key, self.program_uuid, '10', 'null')

        input = [
            '\t'.join([self.org_key, self.program_uuid, '10', 'masters', '1', '2019', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '10', 'audit,verified', '2', '2017', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '10', 'masters', '1', '2019', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '10', 'audit', '1', '2018', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '10', 'audit', '2', '2018', 'False', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '10', 'no-id-professional', '1', 'null', 'False', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '10', 'professional', '1', '2019', 'True', 'False', 'null'])
        ]

        self._check_output_complete_tuple(input, ([self.org_key, self.program_uuid, '10', '2017', 9, 5, 3, 1, 2, 2, False, 'null'],))


class CountProgramCohortEnrollmentsMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = CountProgramCohortEnrollments
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )
        super(CountProgramCohortEnrollmentsMapperTest, self).setUp()

    def test_mapper(self):
        line = '\t'.join([self.org_key, self.program_uuid, '10', '2019', '10', '5', '2', '5', '0', '3', 'False', 'null'])
        self.assert_single_map_output(line, (self.org_key, self.program_uuid, '2019', 'null'), line)


class CountProgramCohortEnrollmentsReducerTest(CohortEnrollmentCountTestMixin, ProgramReportTestMixin, ReducerTestMixin, TestCase):

    task_class = CountProgramCohortEnrollments
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.maxDiff = None
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
            table_name='table',
        )

        super(CountProgramCohortEnrollmentsReducerTest, self).setUp()

    def test_reducer(self):
        self.reduce_key = (self.org_key, self.program_uuid, '2019', 'null')

        input = [
            '\t'.join([self.org_key, self.program_uuid, '10', '2019', '10', '5', '2', '5', '0', '3', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '11', '2019', '1', '1', '0', '0', '0', '1', 'True', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '12', '2019', '5', '3', '2', '0', '0', '3', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '13', '2019', '3', '2', '0', '0', '0', '3', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '14', '2019', '7', '5', '2', '0', '0', '5', 'False', 'null']),
            '\t'.join([self.org_key, self.program_uuid, '15', '2019', '1', '1', '0', '0', '0', '1', 'True', 'null']),
        ]

        self._check_output_complete_tuple(input, (
            (self.build_cohort_enrollment_output(self.org_key, self.program_uuid),),
        ))


class BuildAggregateProgramReportMapperTest(CohortEnrollmentCountTestMixin, ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = BuildAggregateProgramReport
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )
        super(BuildAggregateProgramReportMapperTest, self).setUp()

    def test_map_cohort_enrollment(self):

        input_line = self.build_cohort_enrollment_output(self.org_key, self.program_uuid)

        expected_output = {
            'input_type': 'cohort_enrollments',
            'data': input_line
        }

        self.assert_single_map_output(input_line, (self.org_key, self.program_uuid), expected_output)

    def test_map_program_metadata(self):

        input_line = self.create_program_input(self.org_key, self.program_uuid)
        expected_output = {
            'input_type': 'program_metadata',
            'data': VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER.join([
                self.org_key,
                self.program_title,
                self.program_uuid,
                self.program_type,
                str(self.course_count),
            ])
        }

        self.assert_single_map_output(input_line, (self.org_key, self.program_uuid), expected_output)


class BuildAggregateProgramReportReducerTest(CohortEnrollmentCountTestMixin, ProgramReportTestMixin, ReducerTestMixin, ProgramMultiOutputMapReduceReducerTestMixin, TestCase):

    task_class = BuildAggregateProgramReport
    DATE = datetime(2019, 3, 2).date()

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
            table_name='table',
        )
        super(BuildAggregateProgramReportReducerTest, self).setUp()

    def get_inputs(self, org_key, program_uuid):

        course_cohort = {
            'input_type': 'cohort_enrollments',
            'data': self.build_cohort_enrollment_output(org_key, program_uuid),
        }
        program_metadata = {
            'input_type': 'program_metadata',
            'data': self.create_program_input(org_key, program_uuid)
        }
        return [course_cohort, program_metadata]

    def test_multi_file_reduce(self):
        program_1 = 'e86dc8e3-47cd-4875-87bb-98356e1aa876'
        program_2 = '01015fd3-3b19-48b5-867c-1c78ef43a315'
 
        reduce_keys = [
            (self.org_key, program_1),
            (self.org_key, program_2),
        ]
        columns = self.task_class.get_column_names(self.course_count)
        expected_output = {}
        for program_key in reduce_keys:
            self.reduce_key = program_key
            inputs = self.get_inputs(*program_key)

            # MultiOutputMapReduceJobTasks.reduce() returns an empty tuple
            reducer_output = self._get_reducer_output(inputs)
            self.assertEquals(reducer_output, tuple())

            header = ','.join(columns)
            program_meta = ','.join([program_key[0], self.program_title, program_key[1], self.program_type])
            entry_year = '2019'
            aggregate_values = '6,27,2'  # total learners, total enrollments, total completions
            enrollment_counts = [
                '3,3,0,0,0,0',  # audit
                '1,1,1,1,1,0',  # verified
                '0,0,0,0,0,0',  # professional
                '6,4,4,1,1,0',  # masters
                '6,4,3,2,2,0',  # course completion
            ]

            expected_output[program_key] = [
                header,
                ','.join([
                    program_meta,
                    entry_year,
                    aggregate_values,
                    ','.join(enrollment_counts),
                    'null',
                ])
            ]

        self.validate_output_files(expected_output, 'aggregate_report__{}.csv'.format(self.DATE))
