"""
Tests for event export tasks
"""

import datetime
import json
from collections import defaultdict
from itertools import chain
from unittest import TestCase

import luigi.task
import yaml
from luigi.date_interval import Year
from mock import MagicMock, patch

from edx.analytics.tasks.export.event_exports import EventExportTask
from edx.analytics.tasks.util.tests.opaque_key_mixins import InitializeOpaqueKeysMixin
from edx.analytics.tasks.util.tests.target import FakeTarget


class EventExportTestCaseBase(InitializeOpaqueKeysMixin, TestCase):
    """Base for Tests of EventExportTask."""

    SERVER_NAME_1 = 'FakeServerGroup'
    SERVER_NAME_2 = 'OtherFakeServerGroup'

    def setUp(self):
        self.initialize_ids()
        self.task = self._create_export_task()

    def _create_export_task(self, **kwargs):
        task = EventExportTask(
            mapreduce_engine='local',
            output_root='test://output/',
            config='test://config/default.yaml',
            source=['test://input/'],
            environment='edx',
            interval=Year.parse('2014'),
            gpg_key_dir='test://config/gpg-keys/',
            gpg_master_key='skeleton.key@example.com',
            **kwargs
        )

        task.input_local = MagicMock(return_value=FakeTarget(value=self.CONFIGURATION))
        return task

    def run_mapper_for_server_file(self, server, event_string):
        """Emulate execution of the map function on data emitted by the given server."""
        return self.run_mapper_for_file_path('test://input/{0}/tracking.log'.format(server), event_string)

    def run_mapper_for_file_path(self, path, event_string):
        """Emulate execution of the map function on data read from the given file path."""
        with patch.dict('os.environ', {'map_input_file': path}):
            return [output for output in self.task.mapper(event_string) if output is not None]


class EventExportTestCase(EventExportTestCaseBase):
    """Tests for EventExportTask."""

    EXAMPLE_EVENT = '{"context":{"org_id": "FooX"}, "time": "2014-05-20T00:10:30+00:00","event_source": "server"}'
    EXAMPLE_TIME = '2014-05-20T00:10:30+00:00'
    EXAMPLE_DATE = '2014-05-20'

    # Include some non-standard spacing in this JSON to ensure that the data is not modified in any way.
    EVENT_TEMPLATE = \
        '{{"context":{{"org_id": "{org_id}"}}, "time": "{time}","event_source": "server"}}'  # pep8: disable=E231

    CONFIG_DICT = {
        'organizations': {
            'FooX': {
                'recipients': ['automation@foox.com']
            },
            'BarX': {
                'recipients': ['automation@barx.com'],
                'other_names': [
                    'BazX',
                    'bar'
                ]
            },
            'Bar2X': {
                'recipients': ['automation@bar2x.com'],
                'other_names': [
                    'bar'
                ]
            }
        }
    }
    CONFIGURATION = yaml.dump(CONFIG_DICT)

    def test_org_whitelist_capture(self):
        self.task.init_local()
        self.assertItemsEqual(self.task.org_id_whitelist, ['FooX', 'BarX', 'BazX', 'Bar2X', 'bar'])
        expected_primary_org_id_map = {
            'FooX': ['FooX'],
            'BarX': ['BarX'],
            'BazX': ['BarX'],
            'Bar2X': ['Bar2X'],
            'bar': ['BarX', 'Bar2X'],
        }
        # Compare contents of dicts, but allowing for different order within list values.
        self.assertItemsEqual(self.task.primary_org_ids_for_org_id, expected_primary_org_id_map)
        for key in expected_primary_org_id_map:
            self.assertItemsEqual(self.task.primary_org_ids_for_org_id[key], expected_primary_org_id_map[key])

    def test_limited_orgs(self):
        task = self._create_export_task(org_id=['Bar2X'])
        task.init_local()
        self.assertItemsEqual(task.org_id_whitelist, ['Bar2X', 'bar'])
        self.assertEqual(task.primary_org_ids_for_org_id, {'Bar2X': ['Bar2X'], 'bar': ['Bar2X']})

    def test_ccx_course(self):
        event = {
            "event_type": "/courses/ccx-v1:FooX+CourseX+3T2015+ccx@82/ccx_coach",
            "event_source": "server",
            "time": self.EXAMPLE_TIME,
            "context": {
                "course_id": "ccx-v1:FooX+CourseX+3T2015+ccx@82"
            }
        }
        line = json.dumps(event)

        expected_output = [(
            (self.EXAMPLE_DATE, 'FooX'),
            line
        )]
        self.task.init_local()
        result = self.run_mapper_for_server_file(self.SERVER_NAME_1, line)
        self.assertItemsEqual(result, expected_output)

    def test_mapper(self):
        # The following should produce one output per input:
        expected_single_org_output = [
            (
                (self.EXAMPLE_DATE, 'FooX',),
                self.EVENT_TEMPLATE.format(org_id='FooX', time=self.EXAMPLE_TIME)
            ),
            (
                (self.EXAMPLE_DATE, 'BarX'),
                self.EVENT_TEMPLATE.format(org_id='BarX', time=self.EXAMPLE_TIME)
            ),
            (
                (self.EXAMPLE_DATE, 'BarX'),
                self.EVENT_TEMPLATE.format(org_id='BazX', time=self.EXAMPLE_TIME)
            ),
        ]
        single_org_input = [event_string for _, event_string in expected_single_org_output]

        # The following should produce multiple outputs for each input from the mapper.
        multiple_org_input = [self.EVENT_TEMPLATE.format(org_id='bar', time=self.EXAMPLE_TIME)]
        expected_multiple_org_output = [
            (
                (self.EXAMPLE_DATE, 'BarX'),
                multiple_org_input[0]
            ),
            (
                (self.EXAMPLE_DATE, 'Bar2X'),
                multiple_org_input[0]
            ),
        ]

        # This event should be included in the output, even though it was emitted long ago by a mobile device
        delayed_input = [
            '{{"context":{{"org_id": "{org_id}", "received_at": "{received_time}"}}, '
            '"time": "{time}","event_source": "mobile"}}'.format(
                org_id='FooX',
                received_time=self.EXAMPLE_TIME,
                time='2013-05-20T00:10:30+00:00'
            )
        ]
        expected_delayed_output = [
            (
                (self.EXAMPLE_DATE, 'FooX'),
                delayed_input[0]
            )
        ]

        # The following should produce no output from the mapper.
        excluded_events = [
            self.EVENT_TEMPLATE.format(org_id='OtherOrgX', time=self.EXAMPLE_TIME),
            self.EVENT_TEMPLATE.format(org_id='bar', time='2013-12-31T23:59:59+00:00'),
            self.EVENT_TEMPLATE.format(org_id='bar', time='2015-01-01T00:00:00+00:00'),
            '{invalid json',
        ]

        export_event_template = '{{"context":{{"org_id": "FooX"}},"time":"2014-05-20T00:10:30+00:00","event_source":"server","event":{{"_export": {0}}}}}'
        export_browser_event_template = '{{"context":{{"org_id": "FooX"}},"time":"2014-05-20T00:10:30+00:00","event_source":"server","event":"{{\\\"_export\\\": {0}}}"}}'
        expected_suppress_export_input = [
            # These should be exported:
            '{"context":{"org_id": "FooX"},"time":"2014-05-20T00:10:30+00:00","event_source":"server","event":"bogus event"}',
            export_event_template.format('true'),
            export_browser_event_template.format('true'),
            # These should NOT be exported:
            export_event_template.format('false'),
            export_event_template.format('"n"'),
            export_event_template.format('"f"'),
            export_event_template.format('0'),
            export_event_template.format('"0"'),
            export_event_template.format('"false"'),
            export_event_template.format('"no"'),
            export_browser_event_template.format('false'),
            export_browser_event_template.format('"false"'),
        ]
        expected_suppress_export_output = [
            (('2014-05-20', 'FooX',), expected_suppress_export_input[0]),
            (('2014-05-20', 'FooX',), expected_suppress_export_input[1]),
            (('2014-05-20', 'FooX',), expected_suppress_export_input[2]),
        ]

        input_events = single_org_input + multiple_org_input + delayed_input + excluded_events + expected_suppress_export_input
        expected_output = expected_multiple_org_output + expected_single_org_output + expected_delayed_output + expected_suppress_export_output

        self.task.init_local()

        results = []
        for event_string in input_events:
            results.extend(self.run_mapper_for_server_file(self.SERVER_NAME_1, event_string))

        self.assertItemsEqual(results, expected_output)

    def test_org_from_server_context(self):
        event = {
            'event_source': 'server',
            'context': {
                'org_id': 'FooX'
            }
        }
        self.assertEquals('FooX', self.task.get_org_id(event))

    def test_empty_org_from_server_context(self):
        event = {
            'event_source': 'server',
            'context': {
                'org_id': ''
            }
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_missing_server_context(self):
        event = {
            'event_source': 'server'
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_org_from_course_url(self):
        event = {
            'event_source': 'server',
            'event_type': u'/courses/{}/content'.format(self.course_id)
        }
        self.assertEquals(self.org_id, self.task.get_org_id(event))

    def test_org_from_legacy_course_url(self):
        event = {
            'event_source': 'server',
            'event_type': '/courses/FooX/LearningMath/2014T2/content'
        }
        self.assertEquals('FooX', self.task.get_org_id(event))

    def test_org_from_course_url_with_prefix(self):
        event = {
            'event_source': 'server',
            'event_type': u'/some/garbage/courses/{}/content'.format(self.course_id)
        }
        self.assertEquals(self.org_id, self.task.get_org_id(event))

    def test_org_from_legacy_course_url_with_prefix(self):
        event = {
            'event_source': 'server',
            'event_type': '/some/garbage/courses/FooX/LearningMath/2014T2/content'
        }
        self.assertEquals('garbage', self.task.get_org_id(event))

    def test_implicit_event_without_course_url(self):
        event = {
            'event_source': 'server',
            'event_type': '/any/page'
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_org_from_problem_event(self):
        event = {
            'event_source': 'server',
            'event_type': 'problem_check',
            'event': {
                'problem_id': self.problem_id
            }
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_org_from_legacy_problem_event(self):
        event = {
            'event_source': 'server',
            'event_type': 'problem_check',
            'event': {
                'problem_id': 'i4x://FooX/LearningMath/Otherthings'
            }
        }
        self.assertEquals('FooX', self.task.get_org_id(event))

    def test_problem_without_id(self):
        event = {
            'event_source': 'server',
            'event_type': 'problem_check',
            'event': {
            }
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_org_from_legacy_browser_context(self):
        event = {
            'event_source': 'browser',
            'context': {
                'org_id': 'FooX',
                'course_id': 'FooX/LearningMath/2014T2',
            }
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_org_from_page(self):
        event = {
            'event_source': 'browser',
            'page': u'http://courses.example.com/courses/{}/content'.format(self.course_id)
        }
        self.assertEquals(self.org_id, self.task.get_org_id(event))

    def test_org_from_legacy_page(self):
        event = {
            'event_source': 'browser',
            'page': 'http://courses.example.com/courses/FooX/LearningMath/2014T2/content'
        }
        self.assertEquals('FooX', self.task.get_org_id(event))

    def test_incomplete_org_from_legacy_page(self):
        event = {
            'event_source': 'browser',
            'page': 'http://courses.example.com/courses/FooX/LearningMath'
        }
        self.assertEquals('FooX', self.task.get_org_id(event))

    def test_org_from_legacy_page_with_extra_slash(self):
        event = {
            'event_source': 'browser',
            'page': 'http://courses.example.com//courses/FooX/LearningMath/2014T2/content'
        }
        self.assertEquals('courses', self.task.get_org_id(event))

    def test_no_course_in_page_url(self):
        event = {
            'event_source': 'browser',
            'page': 'http://foo.example.com/any/page'
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_no_event_source(self):
        event = {
            'foo': 'bar'
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_unrecognized_event_source(self):
        event = {
            'event_source': 'task',
        }
        self.assertIsNone(self.task.get_org_id(event))

    def test_output_path_for_key(self):
        path = self.task.output_path_for_key((datetime.date(2015, 1, 1), 'OrgX'))
        self.assertEquals('test://output/orgx/edx/events/2015/orgx-edx-events-2015-01-01.log.gz.gpg', path)

    def test_output_path_for_key_casing(self):
        path = self.task.output_path_for_key((datetime.date(2015, 1, 1), 'orgX'))
        self.assertEquals('test://output/orgx/edx/events/2015/orgx-edx-events-2015-01-01.log.gz.gpg', path)

    def test_local_requirements(self):
        self.assertEquals(self.task.requires_local().url, 'test://config/default.yaml')

    def test_hadoop_requirements(self):
        requirements = luigi.task.flatten(self.task.requires_hadoop())
        for task in requirements:
            if hasattr(task, 'url') and task.url == 'test://config/default.yaml':
                self.fail('Expected config task to be excluded from the hadoop requirements.')

        self.assertEquals(1, len(requirements))

        task = requirements[0]
        self.assertEquals(('test://input/',), task.source)
        # Pattern is difficult to validate since it's read from the config
        # Interval is also difficult to validate since it is expanded by the initializer

        # Some coverage missing here, but it's probably good enough for now

    def test_unrecognized_environment(self):
        self.task.init_local()

        for server in [self.SERVER_NAME_1, self.SERVER_NAME_2]:
            expected_output = [((self.EXAMPLE_DATE, 'FooX'), self.EXAMPLE_EVENT)]
            self.assertItemsEqual(self.run_mapper_for_server_file(server, self.EXAMPLE_EVENT), expected_output)

        self.assertItemsEqual(self.run_mapper_for_server_file('foobar', self.EXAMPLE_EVENT), [])

    def test_odd_file_paths(self):
        self.task.init_local()

        for path in ['something.gz', 'test://input/something.gz']:
            self.assertItemsEqual(self.run_mapper_for_file_path(path, self.EXAMPLE_EVENT), [])

    def test_missing_environment_variable(self):
        self.task.init_local()
        self.assertItemsEqual([output for output in self.task.mapper(self.EXAMPLE_EVENT) if output is not None], [])


class TestEvent():
    DATE = '2014-05-20'

    def __init__(self, org_id=None, course_id=None, url=None, source='server'):
        data = {'context': {}, 'event_source': source, 'time': '2014-05-20T00:10:30+00:00'}

        if org_id:
            data['context']['org_id'] = org_id

        if course_id:
            data['context']['course_id'] = course_id

        if source == 'server':
            data['event_type'] = url or ''
        elif source == 'browser':
            data['page'] = 'https://edx.org' + (url or '')

        self.data = data


class CourseEventExportTestCase(EventExportTestCaseBase):
    """Tests for EventExportTask when specifying courses."""

    CONFIG_DICT = {
        'organizations': {
            'FooX': {
                'recipients': ['automation@foox.com']
            },
            'BarX': {
                'recipients': ['automation@barx.com'],
                'other_names': ['FooX', 'baz'],
                'courses': ['BarX/a/b', 'FooX/a/b']
            },
        }
    }
    CONFIGURATION = yaml.dump(CONFIG_DICT)

    def test_select_courses(self):
        def convert(tuples):
            return [TestEvent(*args).data for args in tuples]

        expected_only_foo = convert([
            ('FooX',),
            ('FooX', 'FooX/c/d'),
            ('FooX', 'FooX/c/d', 'wut'),
            ('FooX', None, '/courses/FooX/c/d'),
            ('FooX', None, '/something/FooX/a/b'),
            (None, None, '/courses/FooX/c/d', 'browser')
        ])

        expected_only_bar = convert([
            ('BarX', 'BarX/a/b'),
            ('BarX', 'BarX/a/b', 'wut'),
            ('BarX', None, '/courses/BarX/a/b'),
            (None, None, '/courses/BarX/a/b', 'browser')
        ])

        expected_both = convert([
            ('FooX', 'FooX/a/b'),
            ('FooX', None, '/courses/FooX/a/b'),
            (None, None, '/courses/FooX/a/b', 'browser')
        ])

        non_expected = convert([
            ('BarX',),
            ('BarX', 'BarX/c/d', 'wut'),
            ('BarX', None, 'wut'),
            ('BarX', None, '/courses/BarX/c/d'),
            ('BarX', None, '/BarX/courses/'),
            ('BarX', None, '/courses/BarX'),
            ('BarX', None, '/courses/BarX/a'),
            ('BarX', None, '/something/BarX/a/b'),
            ('BazX', 'BarX/c/d'),
            (None, None, '/courses/BarX/c/d', 'browser')
        ])

        events = expected_only_foo + expected_only_bar + expected_both + non_expected

        self.task.init_local()

        self.maxDiff = None

        results = defaultdict(list)
        for event in events:
            event_string = json.dumps(event)
            out = self.run_mapper_for_server_file(self.SERVER_NAME_1, event_string)
            for key, value in out:
                results[key[1]].append(json.loads(value))

        self.assertItemsEqual(results['FooX'], expected_only_foo + expected_both)
        self.assertItemsEqual(results['BarX'], expected_only_bar + expected_both)

        combined = list(chain.from_iterable(results.itervalues()))
        for value in non_expected:
            self.assertNotIn(value, combined)
