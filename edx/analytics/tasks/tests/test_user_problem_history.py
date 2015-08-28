"""
Tests for user problem history tasks.
"""

import ddt
import luigi
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.user_problem_history import UserProblemHistoryTask


@ddt.ddt
class UserProblemHistoryTaskTest(unittest.TestCase):
    """ Unit tests for UserProblemHistoryTask """
    COURSE_ID = 'edX/DemoX/Demo_Course'

    def setUp(self):
        self.task_instance = UserProblemHistoryTask(
            output_root='',
            interval=luigi.DateIntervalParameter().parse('2015-08')
        )
        self.maxDiff = None

    def test_mapper(self):
        self.task_instance.init_local()
        lines = [
            '{"event_type": "problem_check", "event": {"grade": 1, "attempts": 1, "max_grade": 1, "problem_id": "i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02"}, "event_source": "server", "context": {"user_id": 4, "course_id": "edX/DemoX/Demo_Course"}, "time": "2015-08-07T16:24:09.214310+00:00"}',
            '{"event_type": "problem_check", "event": {"grade": 0, "attempts": 2, "max_grade": 1, "problem_id": "i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02"}, "event_source": "server", "context": {"user_id": 4, "course_id": "edX/DemoX/Demo_Course"}, "time": "2015-08-08T16:25:10.214310+00:00"}',
            '{"event_type": "problem_check", "event": {"grade": 0, "attempts": 3, "max_grade": 1, "problem_id": "i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02"}, "event_source": "server", "context": {"user_id": 4, "course_id": "edX/DemoX/Demo_Course"}, "time": "2015-08-09T16:26:11.214310+00:00"}',
            '{"event_type": "problem_check", "event": {"grade": 0, "attempts": 4, "max_grade": 1, "problem_id": "i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02"}, "event_source": "server", "context": {"user_id": 4, "course_id": "edX/DemoX/Demo_Course"}, "time": "2015-08-10T16:27:12.214310+00:00"}',
            '{"event_type": "problem_check", "event": {"grade": 1, "attempts": 1, "max_grade": 1, "problem_id": "i4x://edX/DemoX/problem/Sample_Algebraic_Problem"}, "event_source": "server", "context": {"user_id": 4, "course_id": "edX/DemoX/Demo_Course"}, "time": "2015-08-12T13:55:39.210230+00:00"}',
        ]
        mapped_results = [next(self.task_instance.mapper(line)) for line in lines]

        expected = [
            (('2015-08-10', self.COURSE_ID, 4, 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02'),
             (1, 1, '2015-08-07T16:24:09.214310+00:00')),
            (('2015-08-10', self.COURSE_ID, 4, 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02'),
             (1, 0, '2015-08-08T16:25:10.214310+00:00')),
            (('2015-08-10', self.COURSE_ID, 4, 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02'),
             (1, 0, '2015-08-09T16:26:11.214310+00:00')),
            (('2015-08-10', self.COURSE_ID, 4, 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02'),
             (1, 0, '2015-08-10T16:27:12.214310+00:00')),
            (('2015-08-17', self.COURSE_ID, 4, 'i4x://edX/DemoX/problem/Sample_Algebraic_Problem'),
             (1, 1, '2015-08-12T13:55:39.210230+00:00')),
        ]

        self.assertItemsEqual(mapped_results, expected)

    @ddt.data(
        (
            (
                # Input: key
                ('2015-08-10', COURSE_ID, 4, 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02'),
                # Input: attempts
                [
                    (1, 1, '2015-08-07T16:24:09.214310+00:00'),
                    (1, 0, '2015-08-08T16:25:10.214310+00:00'),
                    (1, 0, '2015-08-09T16:26:11.214310+00:00'),
                    (1, 0, '2015-08-10T16:27:12.214310+00:00')
                ]
            ),
            (
                # Output
                '2015-08-10', COURSE_ID, 4, 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02', 4, 0, 1
            ),
        ),
        (
            (
                # Input: key
                ('2015-08-17', COURSE_ID, 4, 'i4x://edX/DemoX/problem/Sample_Algebraic_Problem'),
                # Input: attempts
                [
                    (1, 1, '2015-08-12T13:55:39.210230+00:00')
                ]
            ),
            (
                # Output
                '2015-08-17', COURSE_ID, 4, 'i4x://edX/DemoX/problem/Sample_Algebraic_Problem', 1, 1, 1
            ),
        ),
    )
    @ddt.unpack
    def test_reducer(self, entry, output):
        key = entry[0]
        attempts = entry[1]
        reduced = list(self.task_instance.reducer(key, attempts))

        self.assertEqual(
            reduced,
            # date grouping key, course ID, user ID, problem ID, number of attempts, most recent score, max score
            [output]
        )
