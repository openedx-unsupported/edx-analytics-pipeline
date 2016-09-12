"""
End to end test of grades import.
"""

import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class GradesImportAcceptanceTest(AcceptanceTestCase):
    """End to end test of grades import."""
    NUM_REDUCERS = 1

    def test_grades_import(self):
        self.execute_sql_fixture_file('load_courseware_offlinecomputedgrade.sql')

        self.task.launch([
            'GradesPipelineTask',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
            '--today', '2015-08-22',
        ])

        self.maxDiff = None

        self.validate_aggregate()
        self.validate_per_student()

    def validate_aggregate(self):
        """Ensure the per-course letter grade breakdown is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT course_id, date, letter_grade, num_students, percent_students, is_passing'
                ' FROM grade_breakdown'
                ' ORDER BY letter_grade;'
            )
            results = cursor.fetchall()

        expected = [
            ('course-v1:edX+DemoX+2015', datetime.date(2015, 8, 22), None, 2, 50, 0),
            ('course-v1:edX+DemoX+2015', datetime.date(2015, 8, 22), 'C', 1, 25, 1),
            ('course-v1:edX+DemoX+2015', datetime.date(2015, 8, 22), 'D', 1, 25, 1),
        ]
        self.assertItemsEqual(expected, results)

    def validate_per_student(self):
        """Ensure the per-student grade record is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT course_id, user_id, letter_grade, percent_grade, is_passing'
                ' FROM grades ORDER BY user_id;'
            )
            results = cursor.fetchall()

        expected = [
            ('course-v1:edX+DemoX+2015', 1, None, 0, 0),
            ('course-v1:edX+DemoX+2015', 2, 'C', 64, 1),
            ('course-v1:edX+DemoX+2015', 3, None, 0, 0),
            ('course-v1:edX+DemoX+2015', 4, 'D', 53, 1),
        ]
        self.assertItemsEqual(expected, results)
