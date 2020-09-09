import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.record import DateTimeField, FloatField, IntegerField, Record, StringField


class GradesPersistentCourseGradeRecord(Record):
    id = IntegerField()
    user_id = IntegerField()
    course_id = StringField()
    course_edited_timestamp = DateTimeField()
    course_version = StringField()
    grading_policy_hash = StringField()
    percent_grade = FloatField()
    letter_grade = StringField()
    passed_timestamp = DateTimeField()
    created = DateTimeField()
    modified = DateTimeField()


class TestImportPersistentCourseGradeTask(AcceptanceTestCase):

    ENVIRONMENT = 'acceptance'
    TABLE = 'grades_persistentcoursegrade'

    def load_data_from_file(self):
        """
        External Effect: Drops grades_persistentcoursegrade table and loads it with data from a static file.
        """
        self.import_db.execute_sql_file(
            os.path.join(self.data_dir, 'input', 'load_{table}.sql'.format(table=self.TABLE))
        )

    def test_import_from_mysql(self):
        self.load_data_from_file()
        self.task.launch([
            'ImportPersistentCourseGradeTask',
            '--credentials', self.import_db.credentials_file_url,
            '--destination', self.warehouse_path,
            '--database', self.import_db.database_name,
        ])

        columns = [x[0] for x in GradesPersistentCourseGradeRecord.get_hive_schema()]

        # We deliberately do not do a select * here as the query result would also include the partitioning
        # column dt.
        hive_output = self.hive.execute('SELECT {columns} FROM {table};'.format(
            columns=','.join(columns),
            table=self.TABLE,
        ))

        def map_null_to_hive_null(row):
            return ['\\N' if x == 'NULL' else x for x in row]

        output_rows = [x.split('\t') for x in hive_output.splitlines() if '\t' in x]
        output_rows = map(map_null_to_hive_null, output_rows)

        output_records = [GradesPersistentCourseGradeRecord.from_string_tuple(row) for row in output_rows]

        expected_rows = [
            [
                '1', '1', 'edX/Open_DemoX/edx_demo_course', '\\N', 'version-1', 'grading-policy-1',
                '0.7', 'C', '2017-01-31 00:05:00', '2017-02-01 00:00:00', '2017-02-01 00:00:00',
            ],
            [
                '2', '2', 'edX/Open_DemoX/edx_demo_course', '\\N', 'version-1', 'grading-policy-1',
                '0.8', 'B', '2017-01-31 00:05:00', '2017-02-01 00:00:00', '2017-02-01 00:00:00',
            ],
            [
                '3', '3', 'edX/Open_DemoX/edx_demo_course', '\\N', 'version-1', 'grading-policy-1',
                '0.2', 'Fail', '\\N', '2017-02-01 00:00:00', '2017-02-01 00:00:00',
            ],
            [
                '4', '4', 'edX/Open_DemoX/edx_demo_course', '\\N', 'version-1', 'grading-policy-1',
                '0', '', '2017-01-31T00:05:00', '2017-02-01T00:00:00', '2017-02-01T00:00:00',
            ],
        ]

        expected_records = [GradesPersistentCourseGradeRecord.from_string_tuple(row) for row in expected_rows]

        self.assertItemsEqual(output_records, expected_records)
