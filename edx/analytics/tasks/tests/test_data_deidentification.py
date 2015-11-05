"""
Tests for data deidentification tasks.
"""

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget
import edx.analytics.tasks.data_deidentification as deid
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.url import url_path_join

from mock import MagicMock
from mock import sentinel

import json
import tempfile
import os
import shutil


class TestDataDeidentification(unittest.TestCase):
    """Tests for all data deidentification tasks."""

    def run_task(self, task_cls, source):
        """Runs the task with fake targets."""

        task = task_cls(
            course=sentinel.ignored,
            output_directory=sentinel.ignored,
            data_directory=sentinel.ignored,
        )

        fake_input = [FakeTarget(value=source)]
        task.input = MagicMock(return_value=fake_input)

        output_target = FakeTarget()
        task.output = MagicMock(return_value=output_target)

        task.run()
        return output_target.buffer.read()

    def reformat(self, data):
        """Reformat data to make it like a TSV."""
        return "\n".join(["\t".join(row) for row in data]) + '\n'

    def check_output(self, cls, input_value, expected_value):
        """Compares input and expected values."""
        output = self.run_task(task_cls=cls, source=self.reformat(input_value))
        self.assertEquals(output, self.reformat(expected_value))

    def test_auth_user_deidentification(self):
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
        self.check_output(deid.DeidentifyAuthUserTask, data, expected)

    def test_auth_user_profile_deidentification(self):
        header = ['id', 'user_id', 'name', 'language', 'location',
                  'meta', 'courseware', 'gender',
                  'mailing_address', 'year_of_birth', 'level_of_education', 'goals', 'allow_certificate', 'country',
                  'city', 'bio', 'profile_image_uploaded_at']
        data = [
            header,
            ['123', '123456', 'John Doe', 'English', 'Batcave, USA',
             '{"old_names": [["old name", "Name change", "2015-09-07T02:30:17.735773+00:00"]]}', 'course.xml', 'm',
             '4th Street', '1984', 'hs', 'To be someone', '0', 'NA',
             'ID', 'I like to code', '2015-11-21 22:17:57']
        ]
        expected = [
            header,
            ['123', '273678626', '', '', '',
             '', '', 'm',
             '', '1984', 'hs', 'To be someone', '1', 'NA',
             '', '', '2015-11-21 22:17:57']
        ]
        self.check_output(deid.DeidentifyAuthUserProfileTask, data, expected)

    def test_student_course_enrollment_deidentification(self):
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
        self.check_output(deid.DeidentifyStudentCourseEnrollmentTask, data, expected)

    def test_student_language_proficiency_deidentification(self):
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
        self.check_output(deid.DeidentifyStudentLanguageProficiencyTask, data, expected)

    def test_courseware_student_module_deidentification(self):
        header = ['id', 'module_type', 'module_id', 'student_id',
                  'state', 'grade', 'created', 'modified', 'max_grade', 'done',
                  'course_id']
        data = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '123456',
             '{"attempts": 1, "seed": 1, "done": true}', '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na',
             'course-v1:edX+DemoX+Test_2014'],
        ]
        expected = [
            header,
            ['1', 'problem', 'block-v1:edX+DemoX+Test_2014+type@problem+block@123091b4012312r210r120r12r', '273678626',
             '{"attempts": 1, "seed": 1, "done": true}', '0', '2015-10-13 19:22:24', '2015-10-13 19:40:20', '1', 'na',
             'course-v1:edX+DemoX+Test_2014'],
        ]
        self.check_output(deid.DeidentifyCoursewareStudentModule, data, expected)

    def test_certificates_generated_certificate_deidentification(self):
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
            ['1', '273678626', '', '0.21', 'course-v1:edX+DemoX+Test_2014', 'key', '0', 'notpassing',
             '', '', '', '2015-10-16 12:53:49', '2015-10-16 12:53:49',
             '', 'honor']
        ]
        self.check_output(deid.DeidentifyCertificatesGeneratedCertificate, data, expected)

    def test_teams_deidentification(self):
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
        self.check_output(deid.DeidentifyTeamsTask, data, expected)

    def test_teams_membership_deidentification(self):
        header = ['id', 'user_id', 'team_id', 'date_joined', 'last_activity_at']
        data = [
            header,
            ['1', '123456', '1', '2015-10-13 13:14:41', '2015-10-14 18:41:24']
        ]
        expected = [
            header,
            ['1', '273678626', '1', '2015-10-13 13:14:41', '2015-10-14 18:41:24']
        ]
        self.check_output(deid.DeidentifyTeamsMembershipTask, data, expected)

    def test_verification_status_deidentification(self):
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
        self.check_output(deid.DeidentifyVerificationStatusTask, data, expected)

    def test_wiki_article_deidentification(self):
        header = ['id', 'current_revision_id', 'created', 'modified', 'owner_id', 'group_id', 'group_read',
                  'group_write', 'other_read', 'other_write']
        data = [
            header,
            ['1234', '27567', '2013-08-08 22:00:58', '2013-09-30 16:52:21', 'owner_id', 'group_id', '1',
             '2', '3', '4']
        ]
        expected = [
            header,
            ['1234', '27567', '2013-08-08 22:00:58', '2013-09-30 16:52:21', 'owner_id', 'group_id', '1',
             '2', '3', '4']
        ]
        self.check_output(deid.DeidentifyWikiArticleTask, data, expected)

    def test_wiki_article_revision_deidentification(self):
        header = ['id', 'revision_number', 'user_message', 'automatic_log', 'ip_address', 'user_id', 'modified',
                  'created', 'previous_revision_id', 'deleted', 'locked', 'article_id', 'content', 'title']
        data = [
            header,
            ['23456', '1', 'This is a user message', 'automatic_log', '192.168.1.1', '123456', '2013-08-08 22:00:58',
             '2013-08-08 22:00:58', '123', '0', '0', '123', 'This is the content', 'TITLE']
        ]
        expected = [
            header,
            ['23456', '1', '', '', '', '273678626', '2013-08-08 22:00:58',
             '2013-08-08 22:00:58', '123', '0', '0', '123', 'This is the content', 'TITLE']
        ]
        self.check_output(deid.DeidentifyWikiArticleRevisionTask, data, expected)

    def test_mongo_deidentification(self):
        data = '{"author_id":"123456","author_username":"johndoe","body":"This is a body", ' \
               '"course_id":"course-v1:edX+DemoX+Test_2014","votes":{"down":["123456"],"up":["12345"]}}'
        expected = '{"author_id":"273678626","author_username":"username_273678626","body":"This is a body", ' \
                   '"course_id":"course-v1:edX+DemoX+Test_2014","votes":{"down":["123456"],"up":["12345"]}}'
        output = self.run_task(task_cls=deid.DeidentifyMongoDumpsTask, source=data)
        self.assertDictEqual(json.loads(output), json.loads(expected))


class TestDeidentifyCourseDumpTask(unittest.TestCase):
    """Test for DeidentifyCourseDumpTask."""

    def create_paths(self, course, dates):
        """Setups directory structure and files as expected by DeidentifyCourseDumpTask task."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.dump_root = os.path.join(self.temp_rootdir, "dump_root")
        self.output_root = os.path.join(self.temp_rootdir, "output_root")
        filename_safe_course_id = opaque_key_util.get_filename_safe_course_id(course)
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
        task = deid.DeidentifyCourseDumpTask(course=coursename, dump_root=self.dump_root, output_root=self.output_root)
        self.assertEquals(task.data_directory, url_path_join(self.dump_root, coursename, 'state', '2015-12-06'))
