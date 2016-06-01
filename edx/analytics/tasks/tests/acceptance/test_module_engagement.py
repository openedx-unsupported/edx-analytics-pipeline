# pylint: disable=line-too-long
"""
End to end test of the per-module engagement workflow.
"""

import logging
import datetime
import luigi


from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase, when_elasticsearch_available

log = logging.getLogger(__name__)


class ModuleEngagementAcceptanceTest(AcceptanceTestCase):
    """Ensure engagement data is populated in the result store incrementally."""

    EMPTY_INPUT_FILE = 'module_engagement_acceptance_empty.log'
    INPUT_FILE = 'module_engagement_acceptance_tracking_{date}.log'
    NUM_REDUCERS = 1

    @when_elasticsearch_available
    def test_roster_generation(self):
        for day in range(2, 17):
            fake_date = datetime.date(2015, 4, day)
            if day in (13, 16):
                self.upload_tracking_log(self.INPUT_FILE.format(date=fake_date.strftime('%Y%m%d')), fake_date)
            else:
                self.upload_tracking_log(self.EMPTY_INPUT_FILE, fake_date)

        self.execute_sql_fixture_file('load_auth_user_for_internal_reporting_user.sql')
        self.execute_sql_fixture_file('load_auth_userprofile.sql')
        self.execute_sql_fixture_file('load_course_groups_courseusergroup.sql')
        self.execute_sql_fixture_file('load_course_groups_courseusergroup_users.sql')

        self.task.launch(
            [
                'CourseEnrollmentTableTask',
                '--interval-end', '2015-04-17',
                '--n-reduce-tasks', str(self.NUM_REDUCERS),
            ]
        )

        self.task.launch(
            [
                'ModuleEngagementWorkflowTask',
                '--date', '2015-04-17',
                '--n-reduce-tasks', str(self.NUM_REDUCERS),
            ],
            config_override={
                'module-engagement': {
                    'allow_empty_insert': True
                }
            }
        )

        query = {"query": {"match_all": {}}}
        response = self.elasticsearch.client.search(index=self.elasticsearch.alias, body=query)

        self.assertEquals(response['hits']['total'], 4)

        expected_results = [
            {
                'attempt_ratio_order': 0,
                'cohort': 'Group 2',
                'course_id': 'edX/DemoX/Demo_Course_2',
                'discussion_contributions': 0,
                'email': 'staff@example.com',
                'end_date': '2015-04-17',
                'enrollment_date': '2015-04-16',
                'enrollment_mode': 'honor',
                'name': 'staff',
                'problem_attempts': 0,
                'problems_attempted': 0,
                'problems_completed': 0,
                'segments': [''],
                'start_date': '2015-04-10',
                'username': 'staff',
                'videos_viewed': 1
            },
            {
                'attempt_ratio_order': 6,
                'cohort': u'Group 1',
                'course_id': u'edX/DemoX/Demo_Course',
                'discussion_contributions': 6,
                'email': u'honor@example.com',
                'end_date': u'2015-04-17',
                'enrollment_date': u'2015-04-13',
                'enrollment_mode': u'honor',
                'name': u'honor',
                'problem_attempts': 6,
                'problem_attempts_per_completed': 3.0,
                'problems_attempted': 3,
                'problems_completed': 2,
                'segments': [''],
                'start_date': u'2015-04-10',
                'username': u'honor',
                'videos_viewed': 2
            },
            {
                'attempt_ratio_order': -1,
                'cohort': u'Group 2',
                'course_id': u'edX/DemoX/Demo_Course_2',
                'discussion_contributions': 3,
                'email': u'honor@example.com',
                'end_date': u'2015-04-17',
                'enrollment_date': u'2015-04-13',
                'enrollment_mode': u'honor',
                'name': u'honor',
                'problem_attempts': 1,
                'problems_attempted': 1,
                'problems_completed': 0,
                'segments': [u'highly_engaged'],
                'start_date': u'2015-04-10',
                'username': u'honor',
                'videos_viewed': 1
            },
            {
                'attempt_ratio_order': 0,
                'cohort': u'Group 3',
                'course_id': u'course-v1:edX+DemoX+Demo_Course_2015',
                'discussion_contributions': 6,
                'email': u'honor@example.com',
                'end_date': u'2015-04-17',
                'enrollment_date': u'2015-04-13',
                'enrollment_mode': u'honor',
                'name': u'honor',
                'problem_attempts': 0,
                'problems_attempted': 0,
                'problems_completed': 0,
                'segments': [''],
                'start_date': u'2015-04-10',
                'username': u'honor',
                'videos_viewed': 1
            },
        ]
        self.assertItemsEqual([res['_source'] for res in response['hits']['hits']], expected_results)

        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT course_id, start_date, end_date, metric, range_type, low_value, high_value FROM module_engagement_metric_ranges')
            results = cursor.fetchall()

        self.assertItemsEqual(
            [row for row in results],
            [
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'discussion_contributions', 'low', 0, 6.0),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'discussion_contributions', 'normal', 6.0, None),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts', 'low', 0, 6.0),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts', 'normal', 6.0, None),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts_per_completed', 'low', 0, 3.0),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts_per_completed', 'normal', 3.0, None),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_attempted', 'low', 0, 3.0),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_attempted', 'normal', 3.0, None),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_completed', 'low', 0, 2.0),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_completed', 'normal', 2.0, None),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'videos_viewed', 'low', 0, 2.0),
                ('edX/DemoX/Demo_Course', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'videos_viewed', 'normal', 2.0, None),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'discussion_contributions', 'low', 0, 0.45),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'discussion_contributions', 'normal', 0.45, 2.55),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'discussion_contributions', 'high', 2.55, None),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts', 'low', 0, 0.15),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts', 'normal', 0.15, 0.85),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts', 'high', 0.85, None),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts_per_completed', 'low', 0, None),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts_per_completed', 'normal', None, None),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_attempted', 'low', 0, 0.15),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_attempted', 'normal', 0.15, 0.85),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_attempted', 'high', 0.85, None),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_completed', 'normal', 0.0, None),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'videos_viewed', 'low', 0, 1.0),
                ('edX/DemoX/Demo_Course_2', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'videos_viewed', 'normal', 1.0, None),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'discussion_contributions', 'low', 0, 6.0),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'discussion_contributions', 'normal', 6.0, None),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts_per_completed', 'normal', 0, None),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'videos_viewed', 'low', 0, 1.0),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'videos_viewed', 'normal', 1.0, None),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problem_attempts', 'normal', 0.0, None),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_attempted', 'normal', 0.0, None),
                ('course-v1:edX+DemoX+Demo_Course_2015', datetime.date(2015, 4, 10), datetime.date(2015, 4, 17), 'problems_completed', 'normal', 0.0, None)
            ]
        )

        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT date, course_id, username, entity_type, entity_id, event, count FROM module_engagement')
            results = cursor.fetchall()

        april_thirteenth = datetime.date(2015, 4, 13)
        april_sixteenth = datetime.date(2015, 4, 16)

        self.assertItemsEqual(
            [row for row in results],
            [
                (april_thirteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'discussion', 'cba3e4cd91d0466b9ac50926e495b76f', 'contributed', 3),
                (april_thirteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'video', '8c0028eb2a724f48a074bc184cd8635f', 'viewed', 1),
                (april_thirteenth, 'edX/DemoX/Demo_Course', 'honor', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'viewed', 2),
                (april_thirteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'viewed', 1),
                (april_thirteenth, 'edX/DemoX/Demo_Course_2', 'staff', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'viewed', 1),
                (april_sixteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'discussion', 'cba3e4cd91d0466b9ac50926e495b76f', 'contributed', 3),
                (april_sixteenth, 'course-v1:edX+DemoX+Demo_Course_2015', 'honor', 'video', '8c0028eb2a724f48a074bc184cd8635f', 'viewed', 1),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'discussion', 'cba3e4cd91d0466b9ac50926e495b76f', 'contributed', 6),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/0d759dee4f9d459c8956136dbde55f02', 'attempted', 1),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/75f9562c77bc4858b61f907bb810d974', 'attempted', 2),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/75f9562c77bc4858b61f907bb810d974', 'completed', 1),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/a0effb954cca4759994f1ac9e9434bf4', 'attempted', 3),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'problem', 'i4x://edX/DemoX/problem/a0effb954cca4759994f1ac9e9434bf4', 'completed', 1),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'video', 'i4x-edX-DemoX-video-0b9e39477cf34507a7a48f74be381fdd', 'viewed', 2),
                (april_sixteenth, 'edX/DemoX/Demo_Course', 'honor', 'video', 'i4x-edX-DemoX-video-8c0028eb2a724f48a074bc184cd8635f', 'viewed', 1),
                (april_sixteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'discussion', 'cba3e4cd91d0466b9ac50926e495b76f', 'contributed', 3),
                (april_sixteenth, 'edX/DemoX/Demo_Course_2', 'honor', 'problem', 'i4x://edX/DemoX/problem/a0effb954cca4759994f1ac9e9434bf4', 'attempted', 1),
            ]
        )
