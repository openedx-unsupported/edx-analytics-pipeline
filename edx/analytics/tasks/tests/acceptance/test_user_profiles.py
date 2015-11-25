"""Test the task that saves user profiles to MySQL"""

from datetime import datetime

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase


class UserProfilesAcceptanceTest(AcceptanceTestCase):
    """Test the task that saves user profiles to MySQL"""

    def test_user_activity(self):
        self.maxDiff = None
        self.execute_sql_fixture_file('load_auth_user_and_profiles.sql')
 
        self.task.launch([
            'InsertToMysqlUserProfilesTask',
            '--credentials', self.export_db.credentials_file_url,
        ])

        with self.export_db.cursor() as cursor:
            cursor.execute(
                'SELECT '
                    'id, username, last_login, date_joined, is_staff, email, '
                    'name, gender, year_of_birth, level_of_education '
                'FROM user_profile ORDER BY id'
            )
            results = cursor.fetchall()

        # pylint: disable=line-too-long
        self.assertItemsEqual([
            row for row in results
        ], [
            (1, 'jane', datetime(2014, 1, 18, 0, 0), datetime(2014, 1, 15, 0, 0), 0, 'jane@example.com', 'Jane Doe', 'f', 1980, 'b'),
            (2, 'alex', datetime(2014, 2, 18, 0, 0), datetime(2014, 2, 15, 0, 0), 0, 'alex@example.com', 'Alex Doe', None, 1990, None),
            (3, 'cary', datetime(2014, 3, 18, 0, 0), datetime(2014, 3, 15, 0, 0), 0, 'cary@example.com', 'Cary Doe', 'm', 1985, 'hs'),
            (4, 'erin', datetime(2014, 4, 18, 0, 0), datetime(2014, 4, 15, 0, 0), 1, 'erin@example.com', 'Erin Doe', 'f', 1995, None),
            (5, 'lane', datetime(2014, 5, 18, 0, 0), datetime(2014, 5, 15, 0, 0), 0, 'lane@example.com', 'Lane Doe', 'm', None, 'm'),
        ])
