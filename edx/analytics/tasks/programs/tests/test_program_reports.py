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
    BuildAggregateProgramReport, BuildAggregateProgramReportTask, BuildLearnerProgramReportTask,
    CombineCourseEnrollments, CombineCourseEnrollmentsTask, CountCourseEnrollments, CountCourseEnrollmentsTask,
    CountProgramCohortEnrollments, CountProgramCohortEnrollmentsTask
)


class ProgramReportTestMixin(object):

    org_key = 'UoX'
    program_type = 'masters'
    program_title = 'Test Program'
    program_uuid = '77c98259-de10-4c55-9b89-6b45320a25ba'

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

    def create_enrollment_input(self, org_key, program_uuid, delimeter='\t', *args, **kwargs):
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

        export_row = ''
        for field, value in learner_enrollment_template.items():
            if value is None:
                value = 'null'
            export_row += str(value) + delimeter
        return export_row.rstrip(delimeter)


class ProgramMultiOutputMapReduceReducerTestMixin:

    def create_expected_output(self, inputs):
        """Returns list of lines expected in an output file"""
        columns = self.task_class.get_column_names()

        # first line is header
        header = ','.join(columns)
        expected = [header]
        for input_row in inputs:
            fields = input_row.split('\t')[:len(columns)]
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


class LearnerProgramReportMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = BuildLearnerProgramReportTask

    def setUp(self):
        super(LearnerProgramReportMapperTest, self).setUp()

    def test_map_by_program(self):
        line = self.create_enrollment_input(self.org_key, self.program_uuid)
        self.assert_single_map_output(line, (self.org_key, self.program_uuid), line)


class LearnerProgramReportReducerTest(ProgramReportTestMixin, ReducerTestMixin, ProgramMultiOutputMapReduceReducerTestMixin, TestCase):

    task_class = BuildLearnerProgramReportTask
    DATE = '2019-03-02'

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


class CombineCourseEnrollmentsMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = CombineCourseEnrollments
    DATE = '2019-03-02'

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )

        super(CombineCourseEnrollmentsMapperTest, self).setUp()

    def test_mapper(self):
        line = self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER)
        self.assert_single_map_output(line, (self.org_key, self.program_type, self.program_uuid, self.program_title, '10', 'course-v1:edX+UoX+Test_Course', 'null'), line)


class CombineCourseEnrollmentsReducerTest(ProgramReportTestMixin, ReducerTestMixin, TestCase):

    task_class = CombineCourseEnrollments

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
            table_name='table',
        )

        super(CombineCourseEnrollmentsReducerTest, self).setUp()

    def test_reducer_multiple_tracks(self):
        self.reduce_key = (self.org_key, self.program_type, self.program_uuid, self.program_title, '10', 'course-v1:edX+UoX+Test_Course', 'null')

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER),
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, track='verified'),
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, track='audit')
        ]
        self._check_output_complete_tuple(input, ([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'audit,masters,verified', 'null', False, False, 'null'],))

    def test_reducer_entry_year(self):
        self.reduce_key = (self.org_key, self.program_type, self.program_uuid, self.program_title, '10', 'course-v1:edX+UoX+Test_Course', 'null')

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, date_first_enrolled=str(datetime(2019, 12, 31, 10, 30, 15, 5))),
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, date_first_enrolled=str(datetime(2018, 12, 31, 10, 30, 15, 5)))
        ]
        self._check_output_complete_tuple(input, ([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'masters', 2018, False, False, 'null'],))

    def test_reducer_entry_year_null_enrollment_time(self):
        self.reduce_key = (self.org_key, self.program_type, self.program_uuid, self.program_title, '10', 'course-v1:edX+UoX+Test_Course', 'null')

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, date_first_enrolled='null'),
        ]

        self._check_output_complete_tuple(input, ([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'masters', 'null', False, False, 'null'],))

    def test_reducer_course_completed(self):
        self.reduce_key = (self.org_key, self.program_type, self.program_uuid, self.program_title, '10', 'course-v1:edX+UoX+Test_Course', 'null')

        input = [
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER),
            self.create_enrollment_input(self.org_key, self.program_uuid, delimeter=VERTICA_EXPORT_DEFAULT_FIELD_DELIMITER, completed=True)
        ]
        self._check_output_complete_tuple(input, ([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'masters', 'null', True, False, 'null'],))


class CountCourseEnrollmentsMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = CountCourseEnrollments
    DATE = '2019-03-02'

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )

        super(CountCourseEnrollmentsMapperTest, self).setUp()

    def test_counting(self):
        line = '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'audit,verified', '2018', 'True', 'True', 'null'])
        self.assert_single_map_output(line, (self.org_key, self.program_type, self.program_title, self.program_uuid, '10', '2018', 'null'), line)


class CountCourseEnrollmentReducerTest(ProgramReportTestMixin, ReducerTestMixin, TestCase):

    task_class = CountCourseEnrollments
    DATE = '2019-03-02'

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
            table_name='table',
        )

        super(CountCourseEnrollmentReducerTest, self).setUp()

    def test_reducer(self):
        self.reduce_key = (self.org_key, self.program_type, self.program_title, self.program_uuid, '10', '2019', 'null')

        input = [
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'masters', '2019', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'audit,verified', '2019', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'masters', '2019', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'audit', '2019', 'True', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'audit', '2019', 'False', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'no-id-professional', '2019', 'False', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', 'professional', '2019', 'True', 'False', 'null'])
        ]

        self._check_output_complete_tuple(input, ([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', '2019', len(input), 5, 3, 1, 2, 2, False, 'null'],))


class CountProgramCohortEnrollmentsMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = CountProgramCohortEnrollments
    DATE = '2019-03-02'

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )
        super(CountProgramCohortEnrollmentsMapperTest, self).setUp()

    def test_mapper(self):
        line = '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', '2019', '10', '5', '2', '5', '0', '3', 'False', 'null'])
        self.assert_single_map_output(line, (self.org_key, self.program_type, self.program_title, self.program_uuid, '2019', 'null'), line)


class CountProgramCohortEnrollmentsReducerTest(ProgramReportTestMixin, ReducerTestMixin, TestCase):

    task_class = CountProgramCohortEnrollments
    DATE = '2019-03-02'

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
            table_name='table',
        )

        super(CountProgramCohortEnrollmentsReducerTest, self).setUp()

    def test_reducer(self):
        self.reduce_key = (self.org_key, self.program_type, self.program_title, self.program_uuid, '2019', 'null')

        input = [
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '10', '2019', '10', '5', '2', '5', '0', '3', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '11', '2019', '1', '1', '0', '0', '0', '1', 'True', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '12', '2019', '5', '3', '2', '0', '0', '3', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '13', '2019', '3', '2', '0', '0', '0', '3', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '14', '2019', '7', '5', '2', '0', '0', '5', 'False', 'null']),
            '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '15', '2019', '1', '1', '0', '0', '0', '1', 'True', 'null']),
        ]

        self._check_output_complete_tuple(input, ([
            self.org_key, self.program_type, self.program_title, self.program_uuid, '2019', len(input), 27,
            3, 3, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, 1, 1, 1, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            6, 4, 4, 1, 1, 0, 0, 0, 0, 0,
            6, 4, 3, 2, 2, 0, 0, 0, 0, 0,
            2, 'null'
        ],))


class BuildAggregateProgramReportMapperTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    task_class = BuildAggregateProgramReport
    DATE = '2019-03-02'

    def setUp(self):
        self.DEFAULT_ARGS = dict(
            output_root='output_root',
            date=self.DATE,
            table_name='table',
        )
        super(BuildAggregateProgramReportMapperTest, self).setUp()

    def test_mapper(self):
        line = '\t'.join([self.org_key, self.program_type, self.program_title, self.program_uuid, '2019', '6', '27',
                          '3', '3', '0', '0', '0', '0', '0', '0', '0', '0',
                          '1', '1', '1', '1', '1', '0', '0', '0', '0', '0',
                          '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                          '6', '4', '4', '1', '1', '0', '0', '0', '0', '0',
                          '6', '4', '3', '2', '2', '0', '0', '0', '0', '0',
                          '2', 'null'])
        self.assert_single_map_output(line, (self.org_key, self.program_uuid), line)


class BuildAggregateProgramReportReducerTest(ProgramReportTestMixin, ReducerTestMixin, ProgramMultiOutputMapReduceReducerTestMixin, TestCase):

    task_class = BuildAggregateProgramReport
    DATE = '2019-03-02'

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
            table_name='table',
        )
        super(BuildAggregateProgramReportReducerTest, self).setUp()

    @classmethod
    def get_input(self, org_key, program_uuid):
        return ['\t'.join([org_key, self.program_type, self.program_title, program_uuid, '2019', '6', '27',
                           '3', '3', '0', '0', '0', '0', '0', '0', '0', '0',
                           '1', '1', '1', '1', '1', '0', '0', '0', '0', '0',
                           '0', '0', '0', '0', '0', '0', '0', '0', '0', '0',
                           '6', '4', '4', '1', '1', '0', '0', '0', '0', '0',
                           '6', '4', '3', '2', '2', '0', '0', '0', '0', '0',
                           '2', 'null'])]

    def test_multi_file_reduce(self):
        programs = [
            (self.org_key, 'e86dc8e3-47cd-4875-87bb-98356e1aa876'),
            (self.org_key, '01015fd3-3b19-48b5-867c-1c78ef43a315')
        ]
        columns = self.task_class.get_column_names()
        expected_output = {}
        for program_key in programs:
            self.reduce_key = program_key
            input = self.get_input(*program_key)

            # MultiOutputMapReduceJobTasks.reduce() returns an empty tuple
            reducer_output = self._get_reducer_output(input)
            self.assertEquals(reducer_output, tuple())

            expected_output[program_key] = self.create_expected_output(input)

        self.validate_output_files(expected_output, 'aggregate_report__{}.csv'.format(self.DATE))
