from datetime import datetime
from unittest import TestCase

from edx.analytics.tasks.insights.database_imports import ImportPersistentCourseGradeTask
from edx.analytics.tasks.insights.enrollments import CourseGradeByModeDataTask, CourseGradeByModePartitionTask


class TestCourseGradeByModeDataTask(TestCase):

    def test_requires(self):
        # The CourseGradeByModeDataTask should require the CourseGradeByModePartitionTask
        # and the ImportPersistentCourseGradeTask.
        a_date = datetime(2017, 1, 1)
        the_warehouse_path = '/tmp/foo'
        data_task = CourseGradeByModeDataTask(date=a_date, warehouse_path=the_warehouse_path)

        required_tasks = list(data_task.requires())

        assert CourseGradeByModePartitionTask(date=a_date, warehouse_path=the_warehouse_path) == required_tasks[0]
        assert ImportPersistentCourseGradeTask(import_date=a_date, destination=the_warehouse_path) == required_tasks[1]
