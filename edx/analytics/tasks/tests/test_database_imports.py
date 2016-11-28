"""
Ensure we can write from MySQL to Hive data sources.
"""

import datetime
import textwrap

from mock import patch, Mock

from edx.analytics.tasks.database_imports import ImportStudentCourseEnrollmentTask, ImportIntoHiveTableTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config


class ImportStudentCourseEnrollmentTestCase(unittest.TestCase):
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
            DROP TABLE IF EXISTS student_courseenrollment;
            CREATE EXTERNAL TABLE student_courseenrollment (
                id INT,user_id INT,course_id STRING,created TIMESTAMP,is_active BOOLEAN,mode STRING
            )
            PARTITIONED BY (dt STRING)

            LOCATION 's3://foo/bar/student_courseenrollment';
            ALTER TABLE student_courseenrollment ADD PARTITION (dt = '2014-07-01');
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
        with patch('edx.analytics.tasks.database_imports.HivePartitionTarget') as mock_target:
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
