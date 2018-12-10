"""Test enrollment computations"""

from datetime import datetime
from unittest import TestCase

from edx.analytics.tasks.enterprise.enterprise_enrollments import EnterpriseEnrollmentMysqlTask


class TestEnterpriseEnrollmentMysqlTask(TestCase):
    """Test that the correct columns are in the Enterprise Enrollments test set."""
    def test_query(self):
        expected_columns = (
            'enterprise_id',
            'enterprise_name',
            'lms_user_id',
            'enterprise_user_id',
            'course_id',
            'enrollment_created_timestamp',
            'user_current_enrollment_mode',
            'consent_granted',
            'letter_grade',
            'has_passed',
            'passed_timestamp',
            'enterprise_sso_uid',
            'enterprise_site_id',
            'course_title',
            'course_start',
            'course_end',
            'course_pacing_type',
            'course_duration_weeks',
            'course_min_effort',
            'course_max_effort',
            'user_account_creation_timestamp',
            'user_email',
            'user_username',
            'course_key',
            'user_country_code',
            'last_activity_date',
            'coupon_name',
            'coupon_code',
            'offer',
            'current_grade',
            'course_price',
            'discount_price',
            'unenrollment_timestamp',
        )
        import_task = EnterpriseEnrollmentMysqlTask(
            date=datetime(2017, 1, 1), warehouse_path='/tmp/foo'
        )
        select_clause = import_task.insert_source_task.query().partition('FROM')[0]
        for column in expected_columns:
            assert column in select_clause
