"""
Tests for grade import tasks
"""
import datetime
import ddt
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.grades import (
    PerStudentGradeBreakdownTask,
    LetterGradeBreakdownTask,
)

NULL = '\\N'  # Hive NULL representation


@ddt.ddt
class PerStudentGradeBreakdownTaskTest(unittest.TestCase):
    """ Unit tests for PerStudentGradeBreakdownTask """
    COURSE_ID = 'course-v1:edX+DemoX+2015'

    def setUp(self):
        self.task_instance = PerStudentGradeBreakdownTask()
        self.maxDiff = None

    def test_mapper(self):
        lines = [
            '1\tcourse-v1:edX+DemoX+2015\t2015-01-01\t{"grade": "A", "percent": 0.94, "...": "..."}',
            '2\tcourse-v1:edX+DemoX+2015\t2015-01-01\t{"grade": "C", "percent": 0.72, "...": "..."}',
            '3\tcourse-v1:edX+DemoX+2015\t2015-01-01\t{"grade": null, "percent": 0.31, "...": "..."}',
        ]
        mapped_results = [next(self.task_instance.mapper(line)) for line in lines]

        expected = [
            ((self.COURSE_ID, ), ('1', '2015-01-01', '{"grade": "A", "percent": 0.94, "...": "..."}')),
            ((self.COURSE_ID, ), ('2', '2015-01-01', '{"grade": "C", "percent": 0.72, "...": "..."}')),
            ((self.COURSE_ID, ), ('3', '2015-01-01', '{"grade": null, "percent": 0.31, "...": "..."}')),
        ]

        self.assertItemsEqual(mapped_results, expected)

    @ddt.data(
        (
            ('1', '2015-01-01', '{"grade": "A", "percent": 0.94, "...": "..."}'),  # Input
            (COURSE_ID, '1', 'A', 94, 1),  # Output
        ),
        (
            ('3', '2015-01-01', '{"grade": null, "percent": 0.31, "...": "..."}'),  # Input
            (COURSE_ID, '3', '\\N', 31, 0),  # Output
        ),
    )
    @ddt.unpack
    def test_reducer(self, entry, output):
        key = (self.COURSE_ID, )
        entries = [entry]
        reduced = list(self.task_instance.reducer(key, entries))

        self.assertEqual(
            reduced,
            # course ID, user ID, letter grade, percent grade, is passing
            [output]
        )


class LetterGradeBreakdownTaskTest(unittest.TestCase):
    """ Unit tests for LetterGradeBreakdownTask """
    COURSE_ID = 'course-v1:edX+DemoX+2015'

    def setUp(self):
        self.task_instance = LetterGradeBreakdownTask(output_root="", today=datetime.date(2015, 9, 9))
        self.maxDiff = None

    def test_mapper(self):
        lines = [
            'course-v1:edX+DemoX+2015\t1\tA\t94\t1',
            'course-v1:edX+DemoX+2015\t2\tC\t72\t1',
            'course-v1:edX+DemoX+2015\t3\t\\N\t31\t0',
        ]
        mapped_results = [next(self.task_instance.mapper(line)) for line in lines]

        expected = [
            # ((course_id, ), (user_id, letter_grade, is_passing))
            ((self.COURSE_ID, ), ('1', 'A', '1')),
            ((self.COURSE_ID, ), ('2', 'C', '1')),
            ((self.COURSE_ID, ), ('3', NULL, '0')),
        ]

        self.assertItemsEqual(mapped_results, expected)

    def test_reducer(self):
        key = (self.COURSE_ID, )
        entries = [
            # user_id, letter_grade, is_passing
            ('1', 'A', '1'),
            ('2', 'C', '1'),
            ('3', NULL, '0'),
            ('4', NULL, '0'),
            ('5', 'C', '1'),
            ('6', 'C', '1'),
            ('7', 'C', '1'),
            ('8', 'C', '1'),
            ('9', 'B', '1'),
            ('10', 'B', '1'),
        ]
        reduced = list(self.task_instance.reducer(key, entries))

        self.assertItemsEqual(
            reduced,
            [
                # course_id, today's date, letter_grade, num_students, percent_of_students, is_passing
                (self.COURSE_ID, '2015-09-09', NULL, 2, 20., 0),
                (self.COURSE_ID, '2015-09-09', 'A', 1, 10., 1),
                (self.COURSE_ID, '2015-09-09', 'B', 2, 20., 1),
                (self.COURSE_ID, '2015-09-09', 'C', 5, 50., 1),
            ]
        )
