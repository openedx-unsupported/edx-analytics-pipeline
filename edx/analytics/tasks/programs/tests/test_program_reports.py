import os
import shutil
import tempfile
from collections import OrderedDict
from unittest import TestCase

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.programs.program_reports import BuildLearnerProgramReportTask


class ProgramReportTestMixin(object):

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
        """Create import row matching sqoop output on the learner_enrollments table"""
        learner_enrollment_template = OrderedDict([
            ('authoring_institution', org_key),
            ('program_title', 'Test Program'),
            ('program_uuid', program_uuid),
            ('user_id', 10),
            ('username', 'test_user'),
            ('name', 'User'),
            ('user_key', None),
            ('course_title', 'Test Course'),
            ('course_run_key', 'course-v1:edX+UoX+Test_Course+2T2019'),
            ('external_course_run_key', None),
            ('track', 'masters'),
            ('grade', None),
            ('letter_grade', None),
            ('date_last_unenrolled', None),
            ('current_enrollment_is_active', None),
            ('first_verified_enrollment_time', None),
            ('completed', None),
            ('date_completed', None),
            ('course_key', 'course-v1:edX+UoX+Test_Course'),
        ])
        for key, value in kwargs.items():
            learner_enrollment_template[key] = value

        export_row = ''        
        for field, value in learner_enrollment_template.items():
            if value is None:
                value = 'null'
            export_row += str(value) + '\t'
        return export_row.rstrip('\t')

class LearnerProgramReportMapTest(ProgramReportTestMixin, MapperTestMixin, TestCase):

    def setUp(self):
        self.task_class = BuildLearnerProgramReportTask
        super(LearnerProgramReportMapTest, self).setUp()

    def test_map_by_program(self):
        org_key = 'UoX'
        program_uuid = '77c98259-de10-4c55-9b89-6b45320a25ba'
        line = self.create_enrollment_input(org_key, program_uuid)
        self.assert_single_map_output(line, (org_key, program_uuid), line)


class LearnerProgramReportReduceTest(ProgramReportTestMixin, ReducerTestMixin, TestCase):

    task_class = BuildLearnerProgramReportTask
    DATE = '2019-03-02'

    def setUp(self):
        self.setup_dirs()
        self.DEFAULT_ARGS = dict(
            output_root=self.output_dir,
            date=self.DATE,
        )
        super(LearnerProgramReportReduceTest, self).setUp()

    def validate_output_files(self, expected):
        """Ensure each output file contains the correct data"""
        for program_key in expected.keys():
            filename = os.path.join(self.output_dir, program_key[0], program_key[1], 'learner_report__{}.csv'.format(self.DATE))
            with open(filename) as report_file:
                num_lines = 0
                for line in report_file:
                    self.assertEquals(line.rstrip(), expected[program_key][num_lines])
                    num_lines += 1
                self.assertEquals(num_lines, len(expected[program_key]))

    def create_expected_output(self, inputs):
        """Resturns list of lines expected in an output file"""
        # first line is header
        header = ','.join(BuildLearnerProgramReportTask.get_column_names())
        expected = [header]
        for input_row in inputs:
            fields = input_row.split('\t')[:len(header)]
            expected.append(','.join(fields))
        return expected

    def test_multi_file_reduce(self):
        programs = [
            ('UoX', 'e86dc8e3-47cd-4875-87bb-98356e1aa876'),
            ('UoX', '01015fd3-3b19-48b5-867c-1c78ef43a315')
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
        
        self.validate_output_files(expected_results)
