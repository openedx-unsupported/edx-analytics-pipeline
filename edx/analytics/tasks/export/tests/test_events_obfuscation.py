"""Tests for events obfuscation tasks."""

import textwrap
from unittest import TestCase

from ddt import data, ddt, unpack
from mock import MagicMock

import edx.analytics.tasks.util.eventlog as eventlog
from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.export.events_obfuscation import ObfuscateCourseEventsTask
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin
from edx.analytics.tasks.util.tests.target import FakeTarget
from edx.analytics.tasks.util.tests.test_geolocation import FakeGeoLocation
from edx.analytics.tasks.util.tests.test_obfuscate_util import get_mock_user_info_requirements


class EventsObfuscationBaseTest(InitializeOpaqueKeysMixin, MapperTestMixin, ReducerTestMixin, TestCase):
    """Base class for testing event obfuscation."""

    DATE = '2013-12-17'

    def setUp(self):
        self.task_class = ObfuscateCourseEventsTask
        super(EventsObfuscationBaseTest, self).setUp()
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
        results = {
            'explicit_events': FakeTarget(value=self.reformat(explicit_event_list)),
        }
        self.task.input_local = MagicMock(return_value=results)
        self.task.init_local()
        self.task.geoip = FakeGeoLocation()

        self.task.user_info_requirements = get_mock_user_info_requirements()

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
class EventsObfuscationMapTest(EventsObfuscationBaseTest):
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
        {'event_source': 'server', 'event_type': 'list-instructors'},
    )
    def test_allowed_event_types(self, kwargs):
        # Mapper does no modification (i.e. no obfuscation).  It only filters.
        input_line = self.create_event_log_line(**kwargs)
        mapper_output_line = tuple(self.task.mapper(input_line))[0][1]
        self.assertEquals(input_line, mapper_output_line)


@ddt
class EventLineObfuscationTest(EventsObfuscationBaseTest):
    """Test event obfuscation that is used by reducer method."""

    def compare_event_lines(self, expected_line, reducer_output_line):
        """Compare events as dicts instead of JSON-encoded strings, due to ordering."""
        reducer_output = eventlog.decode_json(reducer_output_line)
        expected_event = eventlog.decode_json(expected_line)
        self.assertDictEqual(expected_event, reducer_output)

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
    def test_simple_obfuscate_event_types(self, kwargs):
        input_line = self.create_event_log_line(**kwargs)
        expected_line = self.create_event_log_line(
            **dict(kwargs, **{'template_name': 'expected_event', 'augmented': {'country_code': 'UNKNOWN'}})
        )
        reducer_output_line = self.task.obfuscate_event_line(input_line)
        self.compare_event_lines(expected_line, reducer_output_line)

    @data(
        # Test remapping.
        (
            {'username': 'audit', 'context': {'user_id': 2}},
            {'username': 'username_2147483648', 'context': {'user_id': 2147483648}}
        ),
        (
            {'username': 'does_not_match_2', 'context': {'user_id': 2}},
            {'username': 'REDACTED_USERNAME', 'context': {'user_id': 2147483648}}
        ),
        (
            {'username': 'audit', 'context': {'user_id': '2'}},
            {'username': 'username_2147483648', 'context': {'user_id': 2147483648}}
        ),
        (
            {'username': 'audit', 'context': {'user_id': ''}},
            {'username': 'username_2147483648', 'context': {'user_id': ''}},
        ),
        (
            {'username': 'unmapped_username', 'context': {'user_id': 12345}},
            {'username': 'REDACTED_USERNAME', 'context': {'user_id': 302000641}}
        ),
        (
            {'context': {'user_id': 2, 'username': 'audit'}},
            {'context': {'user_id': 2147483648, 'username': 'username_2147483648'}},
        ),
        (
            {'context': {'user_id': 2, 'username': 'unmapped_username'}},
            {'context': {'user_id': 2147483648, 'username': 'REDACTED_USERNAME'}},
        ),
        (
            {'event': {
                'user_id': 2, 'username': 'audit', 'instructor': 'staff', 'student': 'unmapped', 'user': 'verified'
            }},
            {'event': {
                'user_id': 2147483648, 'username': 'username_2147483648', 'instructor': 'username_8388608',
                'student': 'REDACTED_USERNAME', 'user': 'username_2147485696'
            }},
        ),
        (
            {"event": "{\"code\": \"6FrbD6Ro5z8\", \"user_id\": 2, \"currentTime\": 630.437320479}"},
            {"event": "{\"code\": \"6FrbD6Ro5z8\", \"user_id\": 2147483648, \"currentTime\": 630.437320479}"}
        ),
        # Test removal.  The following fields should all be removed.
        (
            {
                'ip': '127.0.0.1',
                'host': 'test_host',
                'referer': 'http://courses.edx.org/blah',
                'page': 'http://courses.edx.org/blah',
            },
        ),
        (
            {'context': {
                'host': 'test_host',
                'ip': '127.0.0.1',
                'path': 'http://courses.edx.org/blah',
            }},
            {'context': {}},
        ),
        (
            {'context': {
                'client': {
                    'device': 'test_device',
                    'ip': '127.0.0.1',
                },
            }},
            {'context': {'client': {}}},
        ),
        (
            {'event': {
                'certificate_id': 'test_id',
                'certificate_url': 'test_url',
                'source_url': 'test_url',
                'fileName': 'filename',
                'GET': {},
                'POST': {}
            }},
            {'event': {}}
        ),
        (
            {'event': {
                'answer': {'file_upload_key': 'upload_key'},
                'saved_response': {'file_upload_key': 'upload_key'}
            }},
            {'event': {'answer': {}, 'saved_response': {}}}
        ),
        (
            {"event": "{\"POST\": {\"input_642bed8fadc59b6cc5c2_2_1\": [\"choice_3\"]}, \"GET\": {}}"},
            {"event": "{}"}
        ),
        # Test cleaning.
        (
            {'event': "input_c7c8ee343b37ae4dd8cf_2_1=choice_"},
            {'event': "input_c7c8ee343b37ae4dd8cf_2_1=choice_"}
        ),
        (
            {'event': {'body': 'audit performed by John, +1(805)213-4567  john@example.com'}},
            {'event': {'body': 'audit performed by John, <<PHONE_NUMBER>>  <<EMAIL>>'}},
        ),
        (
            {'event': {
                'username': 'audit',
                'body': 'audit performed by John, +1(805)213-4567  john@example.com'
            }},
            {'event': {
                'username': 'username_2147483648',
                'body': '<<FULLNAME>> performed by <<FULLNAME>>, <<PHONE_NUMBER>>  <<EMAIL>>'
            }},
        ),
        (
            {'event': {
                'user_id': '2',
                'body': 'audit performed by John, +1(805)213-4567  john@example.com'
            }},
            {'event': {
                'user_id': 2147483648,
                'body': '<<FULLNAME>> performed by <<FULLNAME>>, <<PHONE_NUMBER>>  <<EMAIL>>'
            }},
        ),
        (
            {
                'username': 'audit',
                'event': {
                    'body': 'audit performed by John, +1(805)213-4567  john@example.com'
                }
            },
            {
                'username': 'username_2147483648',
                'event': {
                    'body': '<<FULLNAME>> performed by <<FULLNAME>>, <<PHONE_NUMBER>>  <<EMAIL>>'
                }
            },
        ),
        (
            {
                'context': {'user_id': 2},
                'event': {
                    'body': 'audit performed by John, +1(805)213-4567  john@example.com'
                }
            },
            {
                'context': {'user_id': 2147483648},
                'event': {
                    'body': '<<FULLNAME>> performed by <<FULLNAME>>, <<PHONE_NUMBER>>  <<EMAIL>>'
                }
            },
        ),
        (
            {'event': {
                'arbitrary': ['nesting', {
                    'of': 'static',
                    'content': {'performed': 'by staff'},
                    'contact': 'phone +1(805)213-4567  staff@example.com'
                }]
            }},
            {'event': {
                'arbitrary': ['nesting', {
                    'of': '<<FULLNAME>>',
                    'content': {'performed': 'by <<FULLNAME>>'},
                    'contact': 'phone <<PHONE_NUMBER>>  <<EMAIL>>'
                }]
            }},
        ),
    )
    @unpack
    def test_obfuscated_events(self, input_kwargs, output_kwargs={}):
        input_line = self.create_event_log_line(**input_kwargs)
        expected_line = self.create_event_log_line(
            **dict(output_kwargs, **{'template_name': 'expected_event', 'augmented': {'country_code': 'UNKNOWN'}})
        )
        reducer_output_line = self.task.obfuscate_event_line(input_line)
        self.compare_event_lines(expected_line, reducer_output_line)

    def test_augmented_event(self):
        # This is not tested with ddt because we need access to self.task.geoip in the input and expected arguments.
        input_line = self.create_event_log_line(**{'ip': self.task.geoip.ip_address_1})
        expected_line = self.create_event_log_line(
            **{'template_name': 'expected_event', 'augmented': {'country_code': self.task.geoip.country_code_1}}
        )
        reducer_output_line = self.task.obfuscate_event_line(input_line)
        self.compare_event_lines(expected_line, reducer_output_line)

    @data(
        # IPv6:
        (
            {
                'ip': '2601:548:4001:2796:7dad:21a6:77df:d3dc',
            },
        ),
        (
            {
                'ip': '',
            },
        ),
    )
    @unpack
    def test_unaugmented_event(self, input_kwargs):
        input_line = self.create_event_log_line(**input_kwargs)
        expected_line = self.create_event_log_line(**{'template_name': 'expected_event'})
        reducer_output_line = self.task.obfuscate_event_line(input_line)
        self.compare_event_lines(expected_line, reducer_output_line)


class ObfuscateCourseEventsOutputPathTest(EventsObfuscationBaseTest):
    """Test output_path_for_key."""

    def test_output_path_for_key(self):
        self.task.course = 'edX_DemoX_Demo_Course'
        path = self.task.output_path_for_key(self.DATE)
        self.assertEquals(
            '/fake/output/edX_DemoX_Demo_Course/events/edX_DemoX_Demo_Course-events-2013-12-17.log.gz', path
        )
