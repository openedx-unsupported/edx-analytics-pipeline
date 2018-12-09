"""
End to end test of video timelines.
"""

import datetime
import logging

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase

log = logging.getLogger(__name__)


class VideoAcceptanceTest(AcceptanceTestCase):
    """End to end test of video timelines."""

    INPUT_FILE = 'video_timeline.log'

    def test_video_timeline(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 5, 2))

        self.task.launch([
            'InsertToMysqlAllVideoTask',
            '--interval', '2014-05-01-2014-05-06',
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ])

        self.validate_video_info()
        self.validate_video_timeline()

    def validate_video_info(self):
        """Ensure the video information is correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT pipeline_video_id, course_id, encoded_module_id, duration, segment_length,'
                ' users_at_start, users_at_end, total_viewed_seconds FROM video'
                ' ORDER BY pipeline_video_id ASC'
            )
            results = cursor.fetchall()

        expected = [
            (
                'course-v1:edX+DemoX+Test_2014|0b9e39477cf34507a7a48f74be381fdd',
                'course-v1:edX+DemoX+Test_2014',
                '0b9e39477cf34507a7a48f74be381fdd',
                4,
                5,
                1,
                1,
                5,
            ),
            (
                'edX/DemoX/Demo_Course|i4x-edX-DemoX-video-3cb54a11efae4ccc8a0aade24d14b255',
                'edX/DemoX/Demo_Course',
                'i4x-edX-DemoX-video-3cb54a11efae4ccc8a0aade24d14b255',
                24,
                5,
                2,
                2,
                60,
            ),
            (
                'edX/DemoX/Demo_Course|i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f',
                'edX/DemoX/Demo_Course',
                'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f',
                24,
                5,
                0,
                1,
                15,
            ),
            (
                'course-v1:edX+DemoX+Test_2015|0b9e39477cf34507a7a48f74be381fdd1',
                'course-v1:edX+DemoX+Test_2015',
                '0b9e39477cf34507a7a48f74be381fdd1',
                444,
                5,
                1,
                0,
                5
            ),
        ]
        self.assertItemsEqual(expected, results)

    def validate_video_timeline(self):
        """Ensure the video timeline counts are correct."""
        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT pipeline_video_id, segment, num_users, num_views FROM video_timeline'
                ' ORDER BY pipeline_video_id, segment ASC'
            )
            results = cursor.fetchall()

        expected = [
            ('course-v1:edX+DemoX+Test_2014|0b9e39477cf34507a7a48f74be381fdd', 0, 1, 1),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-3cb54a11efae4ccc8a0aade24d14b255', 0, 2, 2),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-3cb54a11efae4ccc8a0aade24d14b255', 1, 2, 3),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-3cb54a11efae4ccc8a0aade24d14b255', 2, 1, 2),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-3cb54a11efae4ccc8a0aade24d14b255', 3, 2, 2),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-3cb54a11efae4ccc8a0aade24d14b255', 4, 2, 3),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 2, 1, 1),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 3, 1, 1),
            ('edX/DemoX/Demo_Course|i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 4, 1, 1),
            ('course-v1:edX+DemoX+Test_2015|0b9e39477cf34507a7a48f74be381fdd1', 0, 1, 1),
        ]
        self.assertItemsEqual(expected, results)
