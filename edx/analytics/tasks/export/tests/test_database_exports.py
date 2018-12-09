"""
Tests for database export tasks
"""
from unittest import TestCase

from mock import Mock
from opaque_keys.edx.locator import CourseLocator

from edx.analytics.tasks.export.database_exports import STUDENT_MODULE_FIELDS, StudentModulePerCourseTask

STATE_MYSQLDUMP = '\'{\\"answer\\": {\\"code\\": \\"print(\\\'hello world\\\')\\\\r\\\\n\\\\t\\", \\"score\\": 1.0}} ' \
    '\\"msg\\": \\"\\\\n<div class=\\\\\\"test\\\\\\">\\\\nTest\\\\n</div>\\\\n\\", \\"num\\": 100}\''

STATE_EXPORT = '{"answer": {"code": "print(\'hello world\')\\\\r\\\\n\\\\t", "score": 1.0}} ' \
    '"msg": "\\\\n<div class=\\\\"test\\\\">\\\\nTest\\\\n</div>\\\\n", "num": 100}'

STUDENT_MODULE_MYSQLDUMP = {
    'id': 10,
    'module_type': 'problem',
    'module_id': 'i4x://a/module/id',
    'student_id': 20,
    'state': STATE_MYSQLDUMP,
    'grade': 'NULL',
    'created': '2012-08-23 18:31:56',
    'modified': '2012-08-23 18:31:56',
    'max_grade': 3,
    'done': 'na',
    'course_id': 'a/course/id'
}


class StudentModulePerCourseTestCase(TestCase):
    """Tests for StudentModulePerCourseTask."""

    def setUp(self):
        self.task = StudentModulePerCourseTask(
            mapreduce_engine='local',
            dump_root='test://dump_root',
            output_root='test://output/',
            output_suffix='test',
        )

    def test_mapper(self):
        data = STUDENT_MODULE_MYSQLDUMP
        line = ','.join(str(data[k]) for k in STUDENT_MODULE_FIELDS)

        key, value = self.task.mapper(line).next()

        course_id = data['course_id']
        self.assertEqual(key, course_id)

        data['state'] = STATE_EXPORT
        export = '\t'.join(str(data[k]) for k in STUDENT_MODULE_FIELDS)
        self.assertEqual(value, export)

    def test_multi_output_reducer(self):
        mock_output_file = Mock()
        rows = [str(i) for i in xrange(5)]
        self.task.multi_output_reducer('key', rows, mock_output_file)

        # Verify addition of new lines at the end of each row
        calls = mock_output_file.write.mock_calls

        def get_argument(call):
            """Pull out first argument in call."""
            return call[1][0]
        result = ''.join(get_argument(c) for c in calls).split('\n')

        result_header = result[0]
        result_body = '\n'.join(result[1:])

        expected_header = '\t'.join(STUDENT_MODULE_FIELDS)
        self.assertEqual(result_header, expected_header)

        expected_body = ''.join(r + '\n' for r in rows)
        self.assertEqual(result_body, expected_body)

    def test_output_path(self):
        course_id = str(CourseLocator(org='Sample', course='Course', run='ID'))
        filename = self.task.output_path_for_key(course_id)
        expected = 'test://output/Sample-Course-ID-courseware_studentmodule-test-analytics.sql'
        self.assertEqual(filename, expected)

    def test_legacy_output_path(self):
        course_id = 'Sample/Course/ID'
        filename = self.task.output_path_for_key(course_id)
        expected = 'test://output/Sample-Course-ID-courseware_studentmodule-test-analytics.sql'
        self.assertEqual(filename, expected)

    def test_empty_output_suffix(self):
        task = StudentModulePerCourseTask(
            mapreduce_engine='local',
            dump_root='test://dump_root',
            output_root='test://output/',
        )

        course_id = 'Sample/Course/ID'
        filename = task.output_path_for_key(course_id)

        expected = 'test://output/Sample-Course-ID-courseware_studentmodule-analytics.sql'
        self.assertEqual(filename, expected)
