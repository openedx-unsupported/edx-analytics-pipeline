"""
Run end-to-end acceptance tests of importing hive data.

"""

import datetime
import logging
import os
import re

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase

log = logging.getLogger(__name__)


class HiveAcceptanceTest(AcceptanceTestCase):
    """Validate the research data export pipeline for a single course and organization."""

    acceptance = 1

    TABLE = 'student_courseenrollment'

    TABLE_COLUMNS = ['id', 'user_id', 'course_id', 'created', 'is_active', 'mode']

    def test_database_import(self):
        """
        Main test function.  Creates sql table, uploads data from sql table to hive,
        and verifies the data is identical.
        """
        self.execute_sql_fixture_file('load_{table}.sql'.format(table=self.TABLE))
        self.hive.drop_table('student_courseenrollment')
        self.run_task()
        sql_validation_data = self.get_clean_validation_data()
        hive_data = self.get_hive_data()
        self.validate_hive_data(hive_data, sql_validation_data)

    def run_task(self):
        """
        Preconditions: Populated student_courseenrollment SQL table in the MySQL database.
        External Effect: imports student_courseenrollment SQL table into hive
                        also stores the data in the --destination directory
        """

        self.task.launch([
            'ImportStudentCourseEnrollmentTask',
            '--credentials', self.import_db.credentials_file_url,
            '--import-date', str(datetime.date.today()),
            '--num-mappers', str(self.NUM_MAPPERS),
            '--destination', self.test_src,
        ])

    def get_hive_data(self):
        """
        Precondition: student_courseenrollment hive table has been populated
        Return the rows in student_courseenrollment in hive
        """
        raw_data = self.hive.execute('SELECT * FROM student_courseenrollment')

        # now clean up hive data to a format to easily compare to the sql data (tsv to csv)
        # a hive row takes the form
        # 1 1   edX/Open_DemoX/edx_demo_course  2014-06-27 16:02:38 true    honor   2014-07-14
        # which we translate from tsv to csv
        # and chop off the automatially added "row created" column at the end
        # 1,1,edX/Open_DemoX/edx_demo_course,2014-06-27 16:02:38,true,honor

        hive_index_dictionary = {}  # key is cleaned data, value is row id; then we can sort rows by id
        data_section = False  # recognize when reading hive rows
        is_first_section = True

        for line in raw_data.splitlines():
            if data_section:
                # data section end condition
                if line.startswith("Time taken"):
                    data_section = False
                    break
                index = int(line.split('\t')[0])
                # drop the last column, as this is an insert timestamp
                cleaned_data = line.rsplit('\t', 1)[0].replace("\t", ",").rstrip()
                hive_index_dictionary[cleaned_data] = index

            # data section start condition
            if line.startswith("OK"):
                # Skip the first section since it doesn't contain data
                if is_first_section:
                    is_first_section = False
                else:
                    data_section = True

        # sort rows by id
        hive_data = sorted(hive_index_dictionary, key=lambda key: hive_index_dictionary[key])
        return hive_data

    def get_clean_validation_data(self):
        validation_rows = []
        sql_file = open(os.path.join(self.data_dir, 'input', 'load_{table}.sql'.format(table=self.TABLE)))

        for line in sql_file.readlines():
            if line.startswith("INSERT INTO"):
                line = line.replace("),", "").replace(");", "").replace("\"", "").replace("\'", "")
                # grab each inserted row
                entry_rows = re.split(r"\(", line)[1:]

                validation_rows = []
                is_active_boolean_index = self.TABLE_COLUMNS.index('is_active')

                for row in entry_rows:
                    columns = row.split(',')

                    # match 1's and 0's with 'true' and 'false'
                    if str(columns[is_active_boolean_index]) == '1':
                        columns[is_active_boolean_index] = 'true'
                    else:
                        columns[is_active_boolean_index] = 'false'

                    validation_rows.append(",".join(columns).rstrip())
                break

        return validation_rows

    def validate_hive_data(self, hive_data, sql_validation_data):
        """
        Parameters: hive_data is fields from the hive query
        hive_data and validation_data are translated to comma delimited fields
        ex: 1,1,edX/Open_DemoX/edx_demo_course,2014-06-27 16:02:38,true,honor

        Validate the hive data is the same as the intial data loaded into sql.
        """

        # assert each row matches
        for i, val in enumerate(sql_validation_data):
            self.assertEqual(sql_validation_data[i], hive_data[i])
