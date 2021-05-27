"""
End to end test of demographic trends.
"""

import datetime
import logging
import os

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.util.url import url_path_join

log = logging.getLogger(__name__)


class EnterpriseUserAcceptanceTest(AcceptanceTestCase):
    """End to end test of demographic trends."""

    CATALOG_DATE = '2016-09-08'
    DATE = '2016-09-08'

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None

    def setUp(self):
        """Loads user and course catalog fixtures."""
        super(EnterpriseUserAcceptanceTest, self).setUp()

        self.prepare_database('lms', self.import_db)

        self.upload_file(
            os.path.join(self.data_dir, 'input', 'user_activity_by_user'),
            url_path_join(
                self.warehouse_path,
                'user_activity_by_user',
                'dt=' + self.DATE,
                'user_activity_by_user_' + self.DATE
            )
        )

    def prepare_database(self, name, database):
        sql_fixture_base_url = url_path_join(self.data_dir, 'input', 'enterprise', name)
        for filename in sorted(os.listdir(sql_fixture_base_url)):
            self.execute_sql_fixture_file(url_path_join(sql_fixture_base_url, filename), database=database)

    def test_enterprise_user_table_generation(self):
        self.launch_task()
        self.validate_enterprise_user_table()

    def launch_task(self):
        """Kicks off the summary task."""
        task_params = [
            'ImportEnterpriseUsersIntoMysql',
            '--date', self.CATALOG_DATE,
        ]

        self.task.launch(task_params)

    def expected_enterprise_user_results(self):
        """Returns expected results"""
        expected = [
            ['0381d3cb033846d48a5cb1475b589d7f', 12, 2, 'ron',
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test2@example.com',
             'test_user2', 'US', datetime.date(2015, 9, 9)],

            ['03fc6c3a33d84580842576922275ca6f', 13, 3, 'hermione',
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test3@example.com',
             'test_user3', 'US', datetime.date(2015, 9, 9)],

            ['0381d3cb033846d48a5cb1475b589d7f', 11, 1, 'harry',
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test@example.com',
             'test_user', 'US', datetime.date(2015, 9, 9)],

            ['0381d3cb033846d48a5cb1475b589d7f', 15, 4, 'ginny',
             datetime.datetime(2015, 2, 12, 23, 14, 35), 'test5@example.com',
             'test_user5', 'US', None],

            ['0381d3cb033846d48a5cb1475b589d7f', 16, 5, 'dory',
             datetime.datetime(2019, 9, 3, 23, 14, 35), 'test6@example.com',
             'test_user6', 'US', None],

        ]

        return [tuple(row) for row in expected]

    def validate_enterprise_user_table(self):
        """Assert the enterprise_user table is as expected."""
        columns = ['enterprise_id', 'lms_user_id', 'enterprise_user_id', 'enterprise_sso_uid',
                   'user_account_creation_timestamp', 'user_email', 'user_username',
                   'user_country_code', 'last_activity_date']
        with self.export_db.cursor() as cursor:
            cursor.execute(
                '''
                  SELECT {columns}
                  FROM   enterprise_user
                '''.format(columns=','.join(columns))
            )
            results = cursor.fetchall()

        expected = self.expected_enterprise_user_results()
        self.assertItemsEqual(expected, results)
