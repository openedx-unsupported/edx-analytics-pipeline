"""Test enrollment computations"""

from datetime import datetime
from unittest import TestCase

from edx.analytics.tasks.enterprise.enterprise_user import EnterpriseUserMysqlTask


class TestEnterpriseUserMysqlTask(TestCase):
    """Test that the correct columns are in the Enterprise Enrollments test set."""
    def test_query(self):
        expected_columns = (
            'enterprise_id',
            'lms_user_id',
            'enterprise_user_id',
            'enterprise_sso_uid',
            'user_account_creation_timestamp',
            'user_email',
            'user_username',
            'user_country_code',
            'last_activity_date',
        )
        import_task = EnterpriseUserMysqlTask(
            date=datetime(2017, 1, 1), warehouse_path='/tmp/foo'
        )
        select_clause = import_task.insert_source_task.query().partition('FROM')[0]
        for column in expected_columns:
            assert column in select_clause
