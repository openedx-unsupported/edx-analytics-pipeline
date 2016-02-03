"""Tests for events deidentification tasks."""

import luigi
import textwrap
from ddt import ddt, data, unpack
from mock import MagicMock, Mock

from edx.analytics.tasks.events_deidentification import DeidentifyCourseEventsTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin
from edx.analytics.tasks.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.tests.target import FakeTarget, FakeTask
import edx.analytics.tasks.util.eventlog as eventlog


class EventsDeidentificationBaseTest(InitializeOpaqueKeysMixin, MapperTestMixin, ReducerTestMixin, unittest.TestCase):
    DATE = '2013-12-17'

    def setUp(self):
        self.task_class = DeidentifyCourseEventsTask
        super(EventsDeidentificationBaseTest, self).setUp()
        self.initialize_ids()
        self.timestamp = "2013-12-17T15:38:32.805444"
        self.event_templates = {
            'event': {
                "username": "staff",
                "host": "test_host",
                "event_source": "server",
                "event_type": 'edx.course.enrollment.activated',
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": 4,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "ip": "127.0.0.1",
                "event": {
                    "course_id": self.course_id,
                    "mode": "honor",
                },
            },
            'expected_event': {
                "username": "username_8388608",
                "event_source": "server",
                "event_type": 'edx.course.enrollment.activated',
                "context": {
                    "course_id": self.course_id,
                    "org_id": self.org_id,
                    "user_id": 8388608,
                },
                "time": "{0}+00:00".format(self.timestamp),
                "event": {
                    "course_id": self.course_id,
                    "mode": "honor",
                },
            },
        }
        self.default_event_template = 'event'
        self.expected_key = self.DATE

    def create_task(self):
        self.task = self.task_class(
            output_root='/fake/output',
            auth_user_path='/fake/input1',
            auth_userprofile_path='/fake/input2',
            explicit_event_whitelist='explicit_events.tsv',
        )
        explicit_event_list = """
            admin browser edx.instructor.report.downloaded
            admin server add-forum-admin

            admin server add-forum-community-TA
            admin server add-forum-mod
            admin server add-instructor
            admin server list-staff
            enrollment server edx.course.enrollment.activated
            # problem server problem_rescore
        """
        auth_user = """
            1 honor
            2 audit
            3 verified
            4 staff
        """
        # TODO: fix this!  We don't have a way of including spaces in the name values, since we're splitting
        # these on whitespace.  Use something else.
        auth_user_profile = """
            1	Honor Student
            2	Audit John
            3	Verified Vera
            4	Static Staff
        """

        results = {
            'explicit_events': FakeTarget(value=self.reformat(explicit_event_list)),
        }
        self.task.input_local = MagicMock(return_value=results)
        self.task.init_local()

        # These keys need to return a Task, whose output() is a Target.
        user_info_setup = {
            'auth_user': FakeTask(value=self.reformat(auth_user, output_delimiter='\x01')),
            'auth_userprofile': FakeTask(value=self.reformat(auth_user_profile, output_delimiter='\x01', input_delimiter='\t')),
        }
        self.task.user_info_requirements = MagicMock(return_value=user_info_setup)

    def reformat(self, string, input_delimiter=' ', output_delimiter='\t'):
        """
        Args:
            string: Input String to be formatted

        Returns:
            Formatted String i.e., blank spaces replaces with tabs (due to the reason that the events_list file
            is supposed to be tab separated

        """
        return textwrap.dedent(string).strip().replace(input_delimiter, output_delimiter)


@ddt
class EventsDeidentificationMapTest(EventsDeidentificationBaseTest):
    """Tests the filtering of events by the mapper.  No events are modified."""
    @data(
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/xblock/block-v1:edX+DemoX+Demo_Course_2015+type@video'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/instructor/api'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/xqueue/1/617ecda9383e45039ab46014d96fc8eb/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/modx'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/wiki/some/thing/_action'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/wiki/_action'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/1/chapter/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/c86e0e59006217ab4fa897d5d634d6e2644b4c74'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74'},
        {'event_source': 'server', 'event_type': 'list-instructors'},
        {'event_source': 'server', 'event_type': 'problem_rescore'},
        {'event_type': None},
        {'event_source': None},
        {'synthesized': {'date': 'blah'}}
    )
    def test_discarded_event_types(self, kwargs):
        line = self.create_event_log_line(**kwargs)
        self.assert_no_map_output_for(line)

    @data(
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/jump_to_id/617ecda9383e45039ab46014d96fc8eb'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/courseware/617ecda9383e45039ab46014d96fc8eb'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/info/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/progress/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/course_wiki/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/about'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/teams'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/617ecda9383e45039ab46014d96fc8eb/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/wiki/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/1'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/0/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/1/chapter/1'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/1/chapter/1/2'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/threads'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/comments'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/upload'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/users'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/4c871d8ebbdf140f2f9b39cdcfdca97ce21b6382/threads/create'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74/inline'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74/threads'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74/threads/617ecda9383e45039ab46014'},
        {'event_source': 'browser', 'event_type': 'edx.instructor.report.downloaded'},
        {'event_source': 'server', 'event_type': 'list-staff'},
    )
    def test_allowed_event_types(self, kwargs):
        input_line = self.create_event_log_line(**kwargs)
        expected_line = self.create_event_log_line(**dict(kwargs, **{'template_name': 'expected_event'}))
        mapper_output_line = tuple(self.task.mapper(input_line))[0][1]
        # Mapper does no modification (i.e. no deidentification).  It only filters.
        self.assertItemsEqual(input_line, mapper_output_line)


@ddt
class EventsDeidentificationReduceTest(EventsDeidentificationBaseTest):

    def compare_event_lines(self, expected_line, reducer_output_line):
        # Cannot compare the lines, because the JSON is not ordered, so convert and compare as dicts.
        reducer_output = eventlog.decode_json(reducer_output_line)
        expected_event = eventlog.decode_json(expected_line)
        self.assertItemsEqual(expected_event, reducer_output)

    @data(
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/jump_to_id/617ecda9383e45039ab46014d96fc8eb'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/courseware/617ecda9383e45039ab46014d96fc8eb'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/info/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/progress/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/course_wiki/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/about'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/teams'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/617ecda9383e45039ab46014d96fc8eb/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/wiki/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/1'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/0/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/1/chapter/1'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/pdfbook/1/chapter/1/2'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/threads'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/comments'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/upload'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/users'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/4c871d8ebbdf140f2f9b39cdcfdca97ce21b6382/threads/create'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74/inline'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74/threads'},
        {'event_type': '/courses/course-v1:edX+DemoX+Demo_Course_2015/discussion/forum/c86e0e59006217ab4fa897d5d634d6e2644b4c74/threads/617ecda9383e45039ab46014'},
        {'event_source': 'browser', 'event_type': 'edx.instructor.report.downloaded'},
        {'event_source': 'server', 'event_type': 'list-staff'},
    )
    def test_simple_deid_event_types(self, kwargs):
        input_line = self.create_event_log_line(**kwargs)
        expected_line = self.create_event_log_line(**dict(kwargs, **{'template_name': 'expected_event'}))
        reducer_output_line = self.task.deidentify_event_line(input_line)
        self.compare_event_lines(expected_line, reducer_output_line)

    @data(
        (
            {'username': 'honor', 'context': {'user_id': 1}},
            {'username': 'username_2048', 'context': {'user_id': 2048}}
        ),
        (
            {'ip': '127.0.0.1', 'host': 'test_host'},
        ),
        (
            {'event': {'user_id': 1, 'username': 'honor'}},
            {'event': {'user_id': 2048, 'username': 'username_2048'}},
        ),
        (
            {'event': {'certificate_id': 'test_id', 'certificate_url': 'test_url', 'source_url': 'test_url', 'fileName': 'filename', 'GET': {}, 'POST': {}}},
            {'event': {}}
        ),
        (
            {'event': {'answer': {'file_upload_key': 'upload_key'}, 'saved_response': {'file_upload_key': 'upload_key'}}},
            {'event': {'answer': {}, 'saved_response': {}}}
        ),
        (
            {'event': "input_c7c8ee343b37ae4dd8cf_2_1=choice_"},
            {'event': "input_c7c8ee343b37ae4dd8cf_2_1=choice_"}
        ),
        (
            {"event": "{\"POST\": {\"input_642bed8fadc59b6cc5c2_2_1\": [\"choice_3\"]}, \"GET\": {}}"},
            {"event": "{}"}
        ),
        (
            {"event": "{\"code\": \"6FrbD6Ro5z8\", \"user_id\": 1, \"currentTime\": 630.437320479}"},
            {"event": "{\"code\": \"6FrbD6Ro5z8\", \"user_id\": 2048, \"currentTime\": 630.437320479}"}
        )
    )
    @unpack
    def test_deidentified_events(self, input_kwargs, output_kwargs={}):
        input_line = self.create_event_log_line(**input_kwargs)
        expected_line = self.create_event_log_line(**dict(output_kwargs, **{'template_name': 'expected_event'}))
        reducer_output_line = self.task.deidentify_event_line(input_line)
        self.compare_event_lines(expected_line, reducer_output_line)


class DeidentifyCourseEventsOutputPathTest(EventsDeidentificationBaseTest):

    def test_output_path_for_key(self):
        self.task.course = 'edX_DemoX_Demo_Course'
        path = self.task.output_path_for_key(self.DATE)
        self.assertEquals('/fake/output/edX_DemoX_Demo_Course/events/edX_DemoX_Demo_Course-events-2013-12-17.log.gz', path)
