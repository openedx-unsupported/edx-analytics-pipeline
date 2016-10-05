"""
Ensure we can write from MySQL to Hive data sources.
"""

import datetime
import textwrap
from unittest import TestCase

from mock import patch, Mock

from edx.analytics.tasks.insights.database_imports import (
    ImportIntoHiveTableTask, ImportPersistentCourseGradeTask, ImportStudentCourseEnrollmentTask,
)
from edx.analytics.tasks.util.tests.config import with_luigi_config


class ImportStudentCourseEnrollmentTestCase(TestCase):
    """Tests to validate ImportStudentCourseEnrollmentTask."""

    def test_base_class(self):
        task = ImportIntoHiveTableTask(**{})
        with self.assertRaises(NotImplementedError):
            task.table_name()

    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        kwargs = {'import_date': datetime.datetime.strptime('2014-07-01', '%Y-%m-%d').date()}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `student_courseenrollment`;
            CREATE EXTERNAL TABLE `student_courseenrollment` (
                `id` INT,`user_id` INT,`course_id` STRING,`created` TIMESTAMP,`is_active` BOOLEAN,`mode` STRING
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/student_courseenrollment';
            ALTER TABLE `student_courseenrollment` ADD PARTITION (dt = '2014-07-01');
            """
        )
        self.assertEquals(query, expected_query)

    def test_overwrite(self):
        kwargs = {'overwrite': True}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        self.assertFalse(task.complete())

    def test_no_overwrite(self):
        # kwargs = {'overwrite': False}
        kwargs = {}
        task = ImportStudentCourseEnrollmentTask(**kwargs)
        with patch('edx.analytics.tasks.insights.database_imports.HivePartitionTarget') as mock_target:
            output = mock_target()
            # Make MagicMock act more like a regular mock, so that flatten() does the right thing.
            del output.__iter__
            del output.__getitem__
            output.exists = Mock(return_value=False)
            self.assertFalse(task.complete())
            self.assertTrue(output.exists.called)
            output.exists = Mock(return_value=True)
            self.assertTrue(task.complete())
            self.assertTrue(output.exists.called)


class ImportPersistentCourseGradeTestCase(TestCase):
    """Tests to validate ImportPersistentCourseGradeTask."""

    @with_luigi_config('database-import', 'destination', 's3://foo/bar')
    def test_query_with_date(self):
        kwargs = {'import_date': datetime.datetime.strptime('2014-07-01', '%Y-%m-%d').date()}
        task = ImportPersistentCourseGradeTask(**kwargs)
        query = task.query()
        expected_query = textwrap.dedent(
            """
            USE default;
            DROP TABLE IF EXISTS `grades_persistentcoursegrade`;
            CREATE EXTERNAL TABLE `grades_persistentcoursegrade` (
                `id` INT,`user_id` INT,`course_id` STRING,`course_edited_timestamp` TIMESTAMP,`course_version` STRING,`grading_policy_hash` STRING,`percent_grade` DECIMAL(10,2),`letter_grade` STRING,`passed_timestamp` TIMESTAMP,`created` TIMESTAMP,`modified` TIMESTAMP
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/grades_persistentcoursegrade';
            ALTER TABLE `grades_persistentcoursegrade` ADD PARTITION (dt = '2014-07-01');
            """
        )
        self.assertEquals(query, expected_query)
