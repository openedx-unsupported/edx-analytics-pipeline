"""
Tests for data obfuscation tasks.
"""

import errno
import json
import logging
import os
import shutil
import tarfile
import tempfile
import xml.etree.ElementTree as ET
from unittest import TestCase

from luigi import LocalTarget
from mock import MagicMock, sentinel

import edx.analytics.tasks.export.data_obfuscation as obfuscate
from edx.analytics.tasks.util.obfuscate_util import reset_user_info_for_testing
from edx.analytics.tasks.util.opaque_key_util import get_filename_safe_course_id
from edx.analytics.tasks.util.tests.target import FakeTarget
from edx.analytics.tasks.util.tests.test_obfuscate_util import get_mock_user_info_requirements
from edx.analytics.tasks.util.url import url_path_join

LOG = logging.getLogger(__name__)


class TestDataObfuscation(TestCase):
    """Tests for all data obfuscation tasks."""

    def run_task(self, task_cls, source):
        """Runs the task with fake targets."""

        task = task_cls(
            course=sentinel.ignored,
            output_directory=sentinel.ignored,
            data_directory=sentinel.ignored,
            auth_user_path=sentinel.ignored,
            auth_userprofile_path=sentinel.ignored,
        )

        fake_input = {'data': [FakeTarget(value=source)]}
        task.input = MagicMock(return_value=fake_input)

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)
        task.user_info_requirements = get_mock_user_info_requirements()
        reset_user_info_for_testing()
        task.run()
        return output_target.buffer.read()

    def reformat(self, data):
        """Reformat data to make it like a TSV."""
        return "\n".join(["\t".join(row) for row in data]) + '\n'

    def check_output(self, cls, input_value, expected_value):
        """Compares input and expected values."""
        output = self.run_task(task_cls=cls, source=self.reformat(input_value))
        self.assertEquals(output, self.reformat(expected_value))

    def test_auth_user_obfuscation(self):
        header = ['id', 'username', 'first_name', 'last_name', 'email', 'password', 'is_staff', 'is_active',
                  'is_superuser', 'last_login', 'date_joined', 'status', 'email_key', 'avatar_type', 'country',
                  'show_country', 'date_of_birth', 'interesting_tags', 'ignored_tags', 'email_tag_filter_strategy',
                  'display_tag_filter_strategy', 'consecutive_days_visit_count']
        data = [
            header,
            ['123456', 'JohnDoe', 'John', 'Doe', 'johndoe@edx.org', '', '1', '1',
             '0', '2015-11-15 22:08:37', '2013-07-08 14:42:50', '', 'NULL', '', '',
             '0', 'NULL', '', '', '0',
             '0', '0']
        ]
        expected = [
            header,
            ['273678626', 'username_273678626', '', '', '', '', '1', '1',
             '0', '2015-11-15 22:08:37', '2013-07-08 14:42:50', '', '', '', '',
             '', '', '', '', '',
             '', '']
        ]
        self.check_output(obfuscate.ObfuscateAuthUserTask, data, expected)

    def test_auth_user_profile_obfuscation(self):
        header = ['id', 'user_id', 'name', 'language', 'location',
                  'meta', 'courseware', 'gender',
                  'mailing_address', 'year_of_birth', 'level_of_education', 'goals', 'country',
                  'city', 'bio', 'profile_image_uploaded_at']
        data = [
            header,
            ['123', '123456', 'John Doe', 'English', 'Batcave, USA',
             '{"old_names": [["old name", "Name change", "2015-09-07T02:30:17.735773+00:00"]]}', 'course.xml', 'm',
             '4th Street', '1984', 'hs', 'To be someone', 'NA',
             'ID', 'I like to code', '2015-11-21 22:17:57']
        ]
        expected = [
            header,
            ['123', '273678626', '', '', '',
             '', '', 'm',
             '', '1984', 'hs', 'To be someone', 'NA',
             '', '', '2015-11-21 22:17:57']
        ]
        self.check_output(obfuscate.ObfuscateAuthUserProfileTask, data, expected)

    def test_student_course_enrollment_obfuscation(self):
        header = ['id', 'user_id', 'course_id', 'created', 'is_active', 'mode']
        data = [
            header,
            ['123', '123456', 'course-v1:edX+DemoX+Test_2014', '2015-07-16 19:19:10', '1', 'honor'],
            ['124', '123457', 'course-v1:edX+DemoX+Test_2014', '2015-07-28 12:41:13', '0', 'verified'],
        ]
        expected = [
            header,
            ['123', '273678626', 'course-v1:edX+DemoX+Test_2014', '2015-07-16 19:19:10', '1', 'honor'],
            ['124', '273680674', 'course-v1:edX+DemoX+Test_2014', '2015-07-28 12:41:13', '0', 'verified'],
        ]
        self.check_output(obfuscate.ObfuscateStudentCourseEnrollmentTask, data, expected)

    def test_student_language_proficiency_obfuscation(self):
        header = ['id', 'user_profile_id', 'code']
        data = [
            header,
            ['1', '145', 'en'],
            ['2', '941', 'zh'],
            ['3', '81724', 'ar'],
        ]
        expected = [
            header,
            ['1', '145', 'en'],
            ['2', '941', 'zh'],
            ['3', '81724', 'ar'],
        ]
        self.check_output(obfuscate.ObfuscateStudentLanguageProficiencyTask, data, expected)

    def test_courseware_student_module_obfuscation(self):
        header = ['id', 'module_type', 'module_id', 'student_id',
                  'state',
                  'grade', 'created', 'modified', 'max_grade', 'done', 'course_id']
        data = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '2',
             '{"correct_map": {"123091b4012312r210r120r12r_2_1": {"hint": "", "hintmode": null, '
             '"correctness": "correct", '
             '"msg": "\\\\nRandom HTML stuff:\\\\n\\\\ntest@example.com\\\\n+1-234-123456 will reach John.",'
             '"answervariable": null, "npoints": 1.0, "queuestate": null}}, '
             '"input_state": {"123091b4012312r210r120r12r_2_1": {}}, "last_submission_time": "2015-12-13T06:17:05Z",'
             '"attempts": 2, "seed": 1, "done": true, '
             '"student_answers": {"123091b4012312r210r120r12r_2_1": '
             '"The answer\\\\r\\\\nwith multiple lines\\\\r\\\\naudit needed\\\\r\\\\n213-4567"}}',
             '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na', 'course-v1:edX+DemoX+Test_2014'],
        ]
        expected = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '2147483648',
             '{"correct_map": {"123091b4012312r210r120r12r_2_1": {"hint": "", "hintmode": null, '
             '"correctness": "correct", '
             '"msg": "\\\\nRandom HTML stuff:\\\\n\\\\n<<EMAIL>>\\\\n<<PHONE_NUMBER>> will reach <<FULLNAME>>.", '
             '"answervariable": null, "npoints": 1.0, "queuestate": null}}, '
             '"input_state": {"123091b4012312r210r120r12r_2_1": {}}, "last_submission_time": "2015-12-13T06:17:05Z", '
             '"attempts": 2, "seed": 1, "done": true, '
             '"student_answers": {"123091b4012312r210r120r12r_2_1": '
             '"The answer\\\\r\\\\nwith multiple lines\\\\r\\\\n<<FULLNAME>> needed\\\\r\\\\n<<PHONE_NUMBER>>"}}',
             '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na', 'course-v1:edX+DemoX+Test_2014'],
        ]
        self.check_output(obfuscate.ObfuscateCoursewareStudentModule, data, expected)

    def test_courseware_student_module_obfuscation_unmapped_id(self):
        header = ['id', 'module_type', 'module_id', 'student_id',
                  'state', 'grade', 'created', 'modified', 'max_grade', 'done', 'course_id']
        data = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '123456',
             '{}', '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na', 'course-v1:edX+DemoX+Test_2014'],
        ]
        expected = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '273678626',
             '{}', '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na', 'course-v1:edX+DemoX+Test_2014'],
        ]
        self.check_output(obfuscate.ObfuscateCoursewareStudentModule, data, expected)

    def test_courseware_student_module_obfuscation_bad_state(self):
        header = ['id', 'module_type', 'module_id', 'student_id',
                  'state', 'grade', 'created', 'modified', 'max_grade', 'done', 'course_id']
        data = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '2',
             'this does not parse', '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na',
             'course-v1:edX+DemoX+Test_2014'],
        ]
        expected = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '2147483648',
             '{}', '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na', 'course-v1:edX+DemoX+Test_2014'],
        ]
        self.check_output(obfuscate.ObfuscateCoursewareStudentModule, data, expected)

    def test_certificates_generated_certificate_obfuscation(self):
        header = ['id', 'user_id', 'download_url', 'grade', 'course_id', 'key', 'distinction', 'status',
                  'verify_uuid', 'download_uuid', 'name', 'created_date', 'modified_date',
                  'error_reason', 'mode']
        data = [
            header,
            ['1', '123456', 'some_url', '0.21', 'course-v1:edX+DemoX+Test_2014', 'key', '0', 'notpassing',
             'verify_uuid', 'download_uuid', 'John Doe', '2015-10-16 12:53:49', '2015-10-16 12:53:49',
             'error_reason', 'honor']
        ]
        expected = [
            header,
            ['1', '273678626', '', '0.21', 'course-v1:edX+DemoX+Test_2014', '', '0', 'notpassing',
             '', '', '', '2015-10-16 12:53:49', '2015-10-16 12:53:49',
             '', 'honor']
        ]
        self.check_output(obfuscate.ObfuscateCertificatesGeneratedCertificate, data, expected)

    def test_teams_obfuscation(self):
        header = ['id', 'team_id', 'name', 'course_id', 'topic_id',
                  'date_created', 'description', 'country', 'language', 'discussion_topic_id', 'last_activity_at',
                  'team_size']
        data = [
            header,
            ['1', 'A-Team-8883d3b43094f0e9e6ec7e190e7600e', 'A Team', 'course-v1:edX+DemoX+Test_2014', 'some_topic',
             '2015-10-13 13:14:41', 'description', 'GB', 'en', 'topic_id', '2015-10-31 21:32:17',
             '8']
        ]
        expected = [
            header,
            ['1', 'A-Team-8883d3b43094f0e9e6ec7e190e7600e', 'A Team', 'course-v1:edX+DemoX+Test_2014', 'some_topic',
             '2015-10-13 13:14:41', 'description', 'GB', 'en', 'topic_id', '2015-10-31 21:32:17',
             '8']
        ]
        self.check_output(obfuscate.ObfuscateTeamsTask, data, expected)

    def test_teams_membership_obfuscation(self):
        header = ['id', 'user_id', 'team_id', 'date_joined', 'last_activity_at']
        data = [
            header,
            ['1', '123456', '1', '2015-10-13 13:14:41', '2015-10-14 18:41:24']
        ]
        expected = [
            header,
            ['1', '273678626', '1', '2015-10-13 13:14:41', '2015-10-14 18:41:24']
        ]
        self.check_output(obfuscate.ObfuscateTeamsMembershipTask, data, expected)

    def test_verification_status_obfuscation(self):
        header = ['timestamp', 'status', 'course_id',
                  'checkpoint_location', 'user_id']
        data = [
            header,
            ['2015-09-03 07:19:10', 'submitted', 'course-v1:edX+DemoX+Test_2014',
             'block-v1:edX+DemoX+Test_2014+type@edx', '123456']
        ]
        expected = [
            header,
            ['2015-09-03 07:19:10', 'submitted', 'course-v1:edX+DemoX+Test_2014',
             'block-v1:edX+DemoX+Test_2014+type@edx', '273678626']
        ]
        self.check_output(obfuscate.ObfuscateVerificationStatusTask, data, expected)

    def test_wiki_article_obfuscation(self):
        header = ['id', 'current_revision_id', 'created', 'modified', 'owner_id', 'group_id', 'group_read',
                  'group_write', 'other_read', 'other_write']
        data = [
            header,
            ['1234', '27567', '2013-08-08 22:00:58', '2013-09-30 16:52:21', 'owner_id', 'group_id', '1',
             '2', '3', '4']
        ]
        expected = [
            header,
            ['1234', '27567', '2013-08-08 22:00:58', '2013-09-30 16:52:21', '', '', '1',
             '2', '3', '4']
        ]
        self.check_output(obfuscate.ObfuscateWikiArticleTask, data, expected)

    def test_wiki_article_revision_obfuscation(self):
        header = ['id', 'revision_number', 'user_message', 'automatic_log', 'ip_address', 'user_id', 'modified',
                  'created', 'previous_revision_id', 'deleted', 'locked', 'article_id', 'content', 'title']
        data = [
            header,
            ['23456', '1', 'This is a user message', 'automatic_log', '192.168.1.1', '4', '2013-08-08 22:00:58',
             '2013-08-22 08:00:58', '123', '0', '0', '123',
             'This is revised by Static Staff and not Vera, and contains staff@example.com. For help, call 381-1234.',
             'Article Title']
        ]
        expected = [
            header,
            ['23456', '1', '', '', '', '8388608', '2013-08-08 22:00:58',
             '2013-08-22 08:00:58', '123', '0', '0', '123',
             'This is revised by <<FULLNAME>> and not Vera, and contains <<EMAIL>>. For help, call <<PHONE_NUMBER>>.',
             'Article Title']
        ]
        self.check_output(obfuscate.ObfuscateWikiArticleRevisionTask, data, expected)

    def test_wiki_article_revision_obfuscation_unmapped_userid(self):
        header = ['id', 'revision_number', 'user_message', 'automatic_log', 'ip_address', 'user_id', 'modified',
                  'created', 'previous_revision_id', 'deleted', 'locked', 'article_id', 'content', 'title']
        data = [
            header,
            ['23456', '1', 'This is a user message', 'automatic_log', '192.168.1.1', '12345', '2013-08-08 22:00:58',
             '2013-08-08 22:00:58', '123', '0', '0', '123',
             'This is revised by Static Staff and not Vera, and contains staff@example.com. For help, call 381-1234.',
             'Article Title']
        ]
        expected = [
            header,
            ['23456', '1', '', '', '', '302000641', '2013-08-08 22:00:58',
             '2013-08-08 22:00:58', '123', '0', '0', '123',
             'This is revised by Static Staff and not Vera, and contains <<EMAIL>>. For help, call <<PHONE_NUMBER>>.',
             'Article Title']
        ]
        self.check_output(obfuscate.ObfuscateWikiArticleRevisionTask, data, expected)

    def test_wiki_article_revision_obfuscation_null_userid(self):
        header = ['id', 'revision_number', 'user_message', 'automatic_log', 'ip_address', 'user_id', 'modified',
                  'created', 'previous_revision_id', 'deleted', 'locked', 'article_id', 'content', 'title']
        data = [
            header,
            ['23456', '1', 'This is a user message', 'automatic_log', '192.168.1.1', 'NULL', '2013-08-08 22:00:58',
             '2013-08-08 22:00:58', '123', '0', '0', '123',
             'This is revised by Static Staff and not Vera, and contains staff@example.com. For help, call 381-1234.',
             'Article Title']
        ]
        expected = [
            header,
            ['23456', '1', '', '', '', 'NULL', '2013-08-08 22:00:58',
             '2013-08-08 22:00:58', '123', '0', '0', '123',
             'This is revised by Static Staff and not Vera, and contains <<EMAIL>>. For help, call <<PHONE_NUMBER>>.',
             'Article Title']
        ]
        self.check_output(obfuscate.ObfuscateWikiArticleRevisionTask, data, expected)

    def test_mongo_obfuscation(self):
        data = '{"author_id":"3","author_username":"deliberately_not_verified",' \
               '"body":"Hi All,\\nI am having trouble. Cell: 321-215-9152\\nEmail: vera@test.edx.org\\n\\nVera",' \
               '"title":"Reply from Vera Verified (vera@test.edx.org)","course_id":"course-v1:edX+DemoX+Test_2014",' \
               '"votes":{"down":["123456"],"up":["12345"],"count":2,"point":0,"down_count":1,"up_count":1},' \
               '"endorsement": {"user_id": "4", "time": {"$date": "2015-09-18T01:01:56.743Z"}},' \
               '"abuse_flaggers":["12345"],"historical_abuse_flaggers":["123456"]}'
        expected = '{"author_id":"2147485696","author_username":"username_2147485696",' \
                   '"body":"Hi All,\\nI am having trouble. Cell: <<PHONE_NUMBER>>\\nEmail: <<EMAIL>>\\n\\n<<FULLNAME>>", ' \
                   '"title":"Reply from <<FULLNAME>> <<FULLNAME>> (<<EMAIL>>)","course_id":"course-v1:edX+DemoX+Test_2014",' \
                   '"votes":{"down":["273678626"],"up":["302000641"],"count":2,"point":0,"down_count":1,"up_count":1},' \
                   '"endorsement": {"user_id": "8388608", "time": {"$date": "2015-09-18T01:01:56.743Z"}},' \
                   '"abuse_flaggers":["302000641"],"historical_abuse_flaggers":["273678626"]}'
        output = self.run_task(task_cls=obfuscate.ObfuscateMongoDumpsTask, source=data)
        self.assertDictEqual(json.loads(output), json.loads(expected))

    def test_mongo_obfuscation_with_nonint_id(self):
        data = '{"author_id":"nonint","author_username":"nonint_user",' \
               '"body":"Hi All,\\nI am having trouble. Cell: 321-215-9152\\nEmail: vera@test.edx.org\\n\\nVera",' \
               '"title":"Reply from Vera Verified (vera@test.edx.org)","course_id":"course-v1:edX+DemoX+Test_2014"}'
        expected = '{"author_id":"nonint","author_username":"nonint_user",' \
                   '"body":"Hi All,\\nI am having trouble. Cell: <<PHONE_NUMBER>>\\nEmail: <<EMAIL>>\\n\\nVera", ' \
                   '"title":"Reply from Vera Verified (<<EMAIL>>)","course_id":"course-v1:edX+DemoX+Test_2014"}'
        output = self.run_task(task_cls=obfuscate.ObfuscateMongoDumpsTask, source=data)
        self.assertDictEqual(json.loads(output), json.loads(expected))

    def test_mongo_obfuscation_with_nonmapped_id(self):
        data = '{"author_id":"12345","author_username":"nonmapped_user",' \
               '"body":"Hi All,\\nI am having trouble. Cell: 321-215-9152\\nEmail: vera@test.edx.org\\n\\nVera",' \
               '"title":"Reply from Vera Verified (vera@test.edx.org)","course_id":"course-v1:edX+DemoX+Test_2014"}'
        expected = '{"author_id":"302000641","author_username":"username_302000641",' \
                   '"body":"Hi All,\\nI am having trouble. Cell: <<PHONE_NUMBER>>\\nEmail: <<EMAIL>>\\n\\nVera", ' \
                   '"title":"Reply from Vera Verified (<<EMAIL>>)","course_id":"course-v1:edX+DemoX+Test_2014"}'
        output = self.run_task(task_cls=obfuscate.ObfuscateMongoDumpsTask, source=data)
        self.assertDictEqual(json.loads(output), json.loads(expected))

    def test_course_structure(self):
        data = json.dumps({
            'block0': {
                'category': 'unknownblock',
                'metadata': {
                    'foo': 'bar',
                    'baz': 10
                }
            },
            'block1': {
                'category': 'course',
                'metadata': {
                    'lti_passports': 'x:foo:bar',
                    'mobile_available': True,
                    'unrecognized': 10
                },
                'children': [
                    'block0'
                ]
            },
            'block2': {
                'category': 'lti',
                'metadata': {
                    'lti_id': 'foo'
                }
            }
        })
        expected = {
            'block0': {
                'category': 'unknownblock',
                'metadata': {},
                'redacted_metadata': ['foo', 'baz']
            },
            'block1': {
                'category': 'course',
                'metadata': {
                    'mobile_available': True
                },
                'redacted_metadata': ['lti_passports', 'unrecognized'],
                'children': [
                    'block0'
                ]
            },
            'block2': {
                'category': 'lti',
                'metadata': {},
                'redacted_metadata': ['lti_id'],
            }
        }
        output = self.run_task(task_cls=obfuscate.CourseStructureTask, source=data)
        self.assertDictEqual(json.loads(output), expected)


class TestObfuscateCourseDumpTask(TestCase):
    """Test for ObfuscateCourseDumpTask."""

    def create_paths(self, course, dates):
        """Setups directory structure and files as expected by ObfuscateCourseDumpTask task."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.dump_root = os.path.join(self.temp_rootdir, "dump_root")
        self.output_root = os.path.join(self.temp_rootdir, "output_root")
        filename_safe_course_id = get_filename_safe_course_id(course)
        for date in dates:
            filepath = os.path.join(self.dump_root, filename_safe_course_id, 'state', date, 'auth_userprofile_file')
            os.makedirs(os.path.dirname(filepath))
            open(filepath, 'a').close()

    def tearDown(self):
        "Remove temp dir. after running the test."
        if os.path.exists(self.temp_rootdir):
            shutil.rmtree(self.temp_rootdir)

    def test_data_directory(self):
        """Test to check whether the data_directory for a course is being set up correctly."""
        coursename = 'edx_demo_course'
        self.create_paths(coursename, dates=['2015-11-25', '2015-11-28', '2015-12-06'])
        task = obfuscate.ObfuscatedCourseDumpTask(
            course=coursename, dump_root=self.dump_root, output_root=self.output_root,
            auth_user_path=sentinel.ignored, auth_userprofile_path=sentinel.ignored,
        )
        self.assertEquals(task.data_directory, url_path_join(self.dump_root, coursename, 'state', '2015-12-06'))


class TestCourseContentTask(TestCase):
    """Ensure sensitive fields are removed from the course content export"""

    COURSE_ID = 'course-v1:edX+DemoX+Test_2014'

    def setUp(self):
        self.archive_root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.archive_root)

        course_id_filename = get_filename_safe_course_id(self.COURSE_ID)
        self.course_root = os.path.join(self.archive_root, course_id_filename)
        os.makedirs(self.course_root)

        with open(os.path.join(self.course_root, 'course.xml'), 'w') as course_file:
            course_file.write('<course url_name="foo" org="edX" course="DemoX"/>')

        policy_dir_path = os.path.join(self.course_root, 'policies', 'foo')
        os.makedirs(policy_dir_path)
        with open(os.path.join(policy_dir_path, 'policy.json'), 'w') as policy_file:
            json.dump({}, policy_file)

    def run_task(self):
        """Runs the task with fake targets."""

        output_archive_root = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, output_archive_root)

        with tempfile.NamedTemporaryFile() as tmp_input_archive:
            with tarfile.open(mode='w:gz', fileobj=tmp_input_archive) as input_archive_file:
                input_archive_file.add(self.archive_root, arcname='')
            tmp_input_archive.seek(0)

            task = obfuscate.CourseContentTask(
                course=sentinel.ignored,
                output_directory=sentinel.ignored,
                data_directory=sentinel.ignored,
                auth_user_path=sentinel.ignored,
                auth_userprofile_path=sentinel.ignored,
            )

            fake_input = {'data': [LocalTarget(path=tmp_input_archive.name)]}
            task.input = MagicMock(return_value=fake_input)

            output_target = FakeTarget()
            task.output = MagicMock(return_value=output_target)
            task.user_info_requirements = get_mock_user_info_requirements()
            reset_user_info_for_testing()
            task.run()

            with tarfile.open(mode='r:gz', fileobj=output_target.buffer) as output_archive_file:
                output_archive_file.extractall(output_archive_root)

        self.output_course_root = os.path.join(output_archive_root, get_filename_safe_course_id(self.COURSE_ID))

    def test_draft_removal(self):
        os.makedirs(os.path.join(self.course_root, 'drafts'))
        self.run_task()
        self.assertTrue(os.path.exists(os.path.join(self.output_course_root, 'course.xml')))
        self.assertFalse(os.path.exists(os.path.join(self.output_course_root, 'drafts')))

    def test_policy_cleaning(self):
        policy_obj = {
            'course/foo': {
                'video_upload_pipeline': {
                    'course_video_upload_token': 'abcdefg'
                },
                'start': '2015-10-05T00:00:00Z',
                'rerandomize': 'always'
            }
        }
        self.write_file('policies/foo/policy.json', json.dumps(policy_obj))
        self.run_task()
        self.assertDictEqual(
            {
                'course/foo': {
                    'start': '2015-10-05T00:00:00Z',
                    'rerandomize': 'always',
                    'redacted_attributes': ['video_upload_pipeline']
                }
            },
            json.loads(self.read_file('policies/foo/policy.json'))
        )

    def test_single_course_xml(self):
        content = '<course url_name="foo" org="edX" course="DemoX">' \
                  '<chapter>' \
                  '<foo a="0" b="1" url_name="bar"><p>hello</p><p>world!</p></foo>' \
                  '</chapter>' \
                  '</course>'

        expected = '<course url_name="foo" org="edX" course="DemoX">' \
                   '<chapter>' \
                   '<foo redacted_attributes="a,b" redacted_children="p" url_name="bar" />' \
                   '</chapter>' \
                   '</course>'
        self.write_file('course.xml', content)
        self.run_task()
        self.assert_xml_equal(expected, self.read_file('course.xml'))

    def write_file(self, relative_path, content):
        """Write a file in the staging area that will be included in the test course package"""
        full_path = os.path.join(self.course_root, relative_path)
        try:
            os.makedirs(os.path.dirname(full_path))
        except OSError as ose:
            if ose.errno != errno.EEXIST:
                raise
        with open(full_path, 'w') as course_file:
            course_file.write(content)

    def read_file(self, relative_path):
        """Read a file from the temporary directory setup to hold the output after the course has been processed"""
        with open(os.path.join(self.output_course_root, relative_path), 'r') as course_file:
            return course_file.read()

    def assert_xml_equal(self, expected, actual):
        """Compare two XML documents to ensure they are equivalent"""
        return self.assert_xml_element_equal(ET.fromstring(expected), ET.fromstring(actual), [])

    def assert_xml_element_equal(self, expected, actual, path):
        """Compare two XML elements to ensure they are equivalent"""
        new_path = path + [actual.tag]
        try:
            self.assertEqual(expected.tag, actual.tag)
            self.assertDictEqual(expected.attrib, actual.attrib)
            self.assertEqual(len(expected), len(actual))
            self.assertEqual(expected.text, actual.text)
            self.assertEqual(expected.tail, actual.tail)
        except AssertionError:
            LOG.error('Difference found at path "%s"', '.'.join(new_path))
            LOG.error('Expected XML: %s', ET.tostring(expected))
            LOG.error('Actual XML: %s', ET.tostring(actual))
            raise

        for expected_child, actual_child in zip(expected, actual):
            self.assert_xml_element_equal(expected_child, actual_child, new_path)

    def test_separate_course_xml(self):
        content = '<course course_image="foo.png" lti_passports="foo" unknown="1">' \
                  '<chapter url_name="abcdefg"/>' \
                  '</course>'

        expected = '<course course_image="foo.png" redacted_attributes="lti_passports,unknown">' \
                   '<chapter url_name="abcdefg"/>' \
                   '</course>'
        self.write_file('course/course.xml', content)
        self.run_task()
        self.assert_xml_equal(expected, self.read_file('course/course.xml'))

    def test_problem_with_children(self):
        self.assert_unchanged_xml(
            'problem/sky.xml',
            '<problem display_name="Sky Color" markdown="null">'
            '<p>What color is the sky?</p>'
            '<multiplechoiceresponse>'
            '<choice correct="false">Red</choice>'
            '<choice correct="true">Blue</choice>'
            '</multiplechoiceresponse>'
            '</problem>'
        )

    def assert_unchanged_xml(self, relative_path, content):
        """Clean the XML and make sure nothing was changed"""
        self.write_file(relative_path, content)
        self.run_task()
        self.assert_xml_equal(content, self.read_file(relative_path))

    def test_subelement_field_mixed_with_children(self):
        # "textbook" is a field that is serialized to a sub-element of course, it should be excluded from further
        # analysis
        content = '<course url_name="foo" org="edX" course="DemoX">' \
                  '<chapter><cleanme cleaned="0"/></chapter>' \
                  '<textbook title="Textbook" book_url="https://s3.amazonaws.com/bucket/foo.txt">' \
                  '<unchanged cleaned="0"/>' \
                  '</textbook>' \
                  '</course>'

        expected = '<course url_name="foo" org="edX" course="DemoX">' \
                   '<chapter><cleanme redacted_attributes="cleaned"/></chapter>' \
                   '<textbook title="Textbook" book_url="https://s3.amazonaws.com/bucket/foo.txt">' \
                   '<unchanged cleaned="0"/>' \
                   '</textbook>' \
                   '</course>'
        self.write_file('course.xml', content)
        self.run_task()
        self.assert_xml_equal(expected, self.read_file('course.xml'))

    def test_unknown_children_status_with_children(self):
        # this block has not declared has_children=True, however, we should log a warning and clean any children if
        # they do exist
        content = '<poll display_name="Has children for some reason">' \
                  '<cleanme cleaned="0"/>' \
                  '</poll>'

        expected = '<poll display_name="Has children for some reason">' \
                   '<cleanme redacted_attributes="cleaned"/>' \
                   '</poll>'
        self.write_file('poll/test.xml', content)
        self.run_task()
        self.assert_xml_equal(expected, self.read_file('poll/test.xml'))
