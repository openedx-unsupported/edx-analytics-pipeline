"""
Tests for event export tasks
"""

import datetime

from luigi.date_interval import Year
from mock import MagicMock, patch
import yaml

from edx.analytics.tasks.event_exports import EventExportTask
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.target import FakeTarget
from edx.analytics.tasks.tests.opaque_key_mixins import InitializeOpaqueKeysMixin


class EventExportTestCase(InitializeOpaqueKeysMixin, unittest.TestCase):
    """Tests for EventExportTask."""

    EXAMPLE_EVENT = '{"context":{"org_id": "FooX"}, "time": "2014-05-20T00:10:30+00:00","event_source": "server"}'
    SERVER_NAME_1 = 'FakeServerGroup'
    SERVER_NAME_2 = 'OtherFakeServerGroup'
    EXAMPLE_TIME = '2014-05-20T00:10:30+00:00'
    EXAMPLE_DATE = '2014-05-20'
    # Include some non-standard spacing in this JSON to ensure that the data is not modified in any way.
    EVENT_TEMPLATE = \
        '{{"context":{{"org_id": "{org_id}"}}, "time": "{time}","event_source": "server"}}'  # pep8: disable=E231
    CONFIG_DICT = {
        'organizations': {
            'FooX': {
                'recipient': 'automation@foox.com'
            },
            'BarX': {
                'recipient': 'automation@barx.com',
                'other_names': [
                    'BazX',
                    'bar'
                ]
            }
        }
    }
    CONFIGURATION = yaml.dump(CONFIG_DICT)

    def setUp(self):
        self.initialize_ids()
        self.task = self._create_export_task()

    def _create_export_task(self, **kwargs):
        task = EventExportTask(
            mapreduce_engine='local',
            output_root='test://output/',
            config='test://config/default.yaml',
            source='test://input/',
            environment='prod',
            interval=Year.parse('2014'),
            gpg_key_dir='test://config/gpg-keys/',
            gpg_master_key='skeleton.key@example.com',
            **kwargs
        )

        task.input_local = MagicMock(return_value=FakeTarget(self.CONFIGURATION))
        return task

    def test_org_whitelist_capture(self):
        self.task.init_local()
        self.assertItemsEqual(self.task.org_id_whitelist, ['FooX', 'BarX', 'BazX', 'bar'])

    def test_limited_orgs(self):
        task = self._create_export_task(org_id=['FooX', 'bar'])
        task.init_local()
        self.assertItemsEqual(task.org_id_whitelist, ['FooX', 'bar'])

    def test_mapper(self):
        expected_output = [
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
            (
                (self.EXAMPLE_DATE, 'BarX'),
                self.EVENT_TEMPLATE.format(org_id='bar', time=self.EXAMPLE_TIME)
            ),
        ]

        excluded_events = [
            (
                (self.EXAMPLE_DATE, 'OtherOrgX'),
                self.EVENT_TEMPLATE.format(org_id='OtherOrgX', time=self.EXAMPLE_TIME)
            ),
            (
                (datetime.date(2013, 12, 31), 'bar'),
                self.EVENT_TEMPLATE.format(org_id='bar', time='2013-12-31T23:59:59+00:00')
            ),
            (
                (datetime.date(2015, 1, 1), 'bar'),
                self.EVENT_TEMPLATE.format(org_id='bar', time='2015-01-01T00:00:00+00:00')
            ),
            (
                (datetime.date(2015, 1, 1), 'bar'),
                '{invalid json'
            )
        ]

        input_events = expected_output + excluded_events

        self.task.init_local()

        results = []
        for key, event_string in input_events:
            results.extend(self.run_mapper_for_server_file(self.SERVER_NAME_1, event_string))

        self.assertItemsEqual(results, expected_output)

    def run_mapper_for_server_file(self, server, event_string):
        """Emulate execution of the map function on data emitted by the given server."""
        return self.run_mapper_for_file_path('test://input/{0}/tracking.log'.format(server), event_string)

    def run_mapper_for_file_path(self, path, event_string):
        """Emulate execution of the map function on data read from the given file path."""
        with patch.dict('os.environ', {'map_input_file': path}):
            return [output for output in self.task.mapper(event_string) if output is not None]

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
            'event_type': '/courses/{}/content'.format(self.course_id)
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
            'event_type': '/some/garbage/courses/{}/content'.format(self.course_id)
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
            'page': 'http://courses.example.com/courses/{}/content'.format(self.course_id)
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
        requirements = self.task.requires_hadoop()
        for task in requirements:
            if hasattr(task, 'url') and task.url == 'test://config/default.yaml':
                self.fail('Expected config task to be excluded from the hadoop requirements.')

        self.assertEquals(1, len(requirements))

        task = requirements[0]
        self.assertEquals('test://input/', task.source)
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
