"""Test course list tasks."""
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime
from unittest import TestCase
from urllib import urlencode

import httpretty
from ddt import data, ddt, unpack

from edx.analytics.tasks.common.tests.map_reduce_mixins import MapperTestMixin, ReducerTestMixin
from edx.analytics.tasks.insights.course_list import (
    CourseListApiDataTask, CourseListPartitionTask, PullCourseListApiData
)

log = logging.getLogger(__name__)


class CourseListTestMixin(object):
    """Common code between the the CourseList task tests"""

    task_class = CourseListApiDataTask

    def setUp(self):
        self.setup_dirs()
        super(CourseListTestMixin, self).setUp()

    def create_task(self, **kwargs):
        """Create the task"""
        self.task = self.task_class(
            output_root=self.output_dir,
            **kwargs
        )

    def setup_dirs(self):
        """Create temp input and output dirs."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_rootdir, "output")
        os.mkdir(self.output_dir)
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)


class CourseListApiDataTaskTest(CourseListTestMixin, TestCase):
    """Tests the CourseBlocksApiDataTask basic functions. """

    def test_complete(self):
        self.create_task()
        self.assertFalse(self.task.complete())

        # Create the output_root/_SUCCESS file
        with open(os.path.join(self.output_dir, '_SUCCESS'), 'w') as success:
            success.write('')
        self.assertTrue(self.task.output().exists())
        self.assertTrue(self.task.complete())


@ddt
class CourseListApiDataMapperTaskTest(CourseListTestMixin, MapperTestMixin, TestCase):
    """Tests the CourseListApiDataTask mapper output"""

    @data(
        '{"abc": "def"}',
        '{"id": null}',
        '{"course_id": null}',
        '{"results": []}',
        '{"results": [{}]}',
        '{"results": [{"abc": "def"}]}',
    )
    def test_no_map_output(self, input_data):
        self.assert_no_map_output_for(input_data)

    @data(
        ('{"id": "def"}', 'def', dict(course_id='def')),  # dogwood API format
        ('{"id": "def", "name": "DEF"}', 'def', dict(course_id='def', name="DEF")),
        ('{"course_id": "abc"}', 'abc', dict(course_id='abc')),  # eucalyptus API format
        ('{"course_id": "abc", "name": "ABC"}', 'abc', dict(course_id='abc', name="ABC")),
    )
    @unpack
    def test_map_output(self, input_data, reduce_key, expected_output):
        self.assert_single_map_output_load_jsons(
            input_data,
            reduce_key,
            expected_output,
        )


@ddt
class CourseListApiDataReducerTaskTest(CourseListTestMixin, ReducerTestMixin, TestCase):
    """Tests the CourseListApiDataTask reducer output"""

    @data(
        (
            (), (),
        ),
        (
            (dict(
                course_id='abc',
                name='name',
                org='org',
                number='number',
                blocks_url='http://blocks.url',
                short_description='hi',
                enrollment_start='2016-08-24 01:01:12.0',
                enrollment_end='2017-08-24 01:01:12.0',
                start='2016-09-24 01:01:12.0',
                end='2017-09-24 01:01:12.0',
                start_display='soon',
                start_type='string',
                effort='lots',
                pacing='instructor',
            ), dict(
                course_id='def',
                name='name2',
                org='org2',
                number='number2',
                blocks_url='http://blocks.url2',
                short_description='hi2',
                enrollment_start='2016-08-24 02:02:22.0',
                enrollment_end='2017-08-24 02:02:22.0',
                start='2016-09-24 02:02:22.0',
                end='2017-09-24 02:02:22.0',
                start_display='2016-08-24',
                start_type='timestamp',
                effort='minimal',
                pacing='self',
            ),),
            (('abc', 'name', 'org', 'number', 'http://blocks.url', 'hi', '2016-08-24 01:01:12.000000',
              '2017-08-24 01:01:12.000000', '2016-09-24 01:01:12.000000', '2017-09-24 01:01:12.000000',
              'soon', 'string', 'lots', 'instructor'),
             ('def', 'name2', 'org2', 'number2', 'http://blocks.url2', 'hi2', '2016-08-24 02:02:22.000000',
              '2017-08-24 02:02:22.000000', '2016-09-24 02:02:22.000000', '2017-09-24 02:02:22.000000',
              '2016-08-24', 'timestamp', 'minimal', 'self')),
        )
    )
    @unpack
    def test_reducer_output(self, input_data, expected_tuples):
        self._check_output_complete_tuple(
            input_data,
            expected_tuples,
        )


@ddt
class PullCourseListApiDataTest(TestCase):
    """Tests the PullCourseListApiData task."""

    task_class = PullCourseListApiData
    auth_url = 'http://localhost:8000/oauth2/access_token/'
    api_url = 'http://localhost:8000/api/courses/v1/courses/'

    def setUp(self):
        super(PullCourseListApiDataTest, self).setUp()
        self.setup_dirs()
        self.create_task()
        httpretty.reset()

    def create_task(self, **kwargs):
        """Create the task."""
        self.task = self.task_class(
            api_root_url=self.api_url,
            warehouse_path=self.cache_dir,
            **kwargs
        )
        return self.task

    def setup_dirs(self):
        """Create temp cache dir."""
        self.temp_rootdir = tempfile.mkdtemp()
        self.cache_dir = os.path.join(self.temp_rootdir, "cache")
        os.mkdir(self.cache_dir)
        self.addCleanup(self.cleanup, self.temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    def mock_api_call(self, method, url, status_code=200, body='', **kwargs):
        """Register the given URL, and send data as a JSON string."""
        if isinstance(body, dict):
            body = json.dumps(body)

        log.debug('register_uri(%s, %s, %s, %s, %s)', method, url, body, status_code, kwargs)
        httpretty.enable()
        httpretty.register_uri(
            method, url, body=body, status=status_code, **kwargs
        )

    def test_cache_and_pagination(self):
        # The cache is clear, and the task is not complete
        self.assertFalse(self.task.complete())

        # Mock the API call with paginated and non-paginated URLs
        body = dict(results=[dict(id=0)])
        pages = (1, 2, 3)
        for mock_api in (True, False):

            # First, we mock the API calls, to populate the cache
            if mock_api:
                params = {'page_size': 100}
                self.mock_api_call('POST', self.auth_url, body=dict(access_token='token', expires_in=2000))

                # Final page is not paginated
                self.mock_api_call('GET', '{url}?{params}'.format(url=self.api_url, params=urlencode(params)),
                                   body=body,
                                   content_type='application/json')

                # But the first pages are
                for page in pages:
                    params['page'] = page
                    cur_page = '{url}?{params}'.format(url=self.api_url, params=urlencode(params))

                    params['page'] = page + 1
                    next_page = '{url}?{params}'.format(url=self.api_url, params=urlencode(params))
                    body['pagination'] = dict(next=next_page)
                    self.mock_api_call('GET', cur_page,
                                       body=body,
                                       match_querystring=True,
                                       content_type='application/json')

                self.task.run()
                self.assertTrue(self.task.complete())

            # Next, we clear the API mocks, and create a new task, and read from the cache
            else:
                httpretty.reset()

                # Create a new task with the same arguments - is already complete
                old_task = self.task
                new_task = self.create_task()
                self.assertTrue(new_task.complete())

        # Ensure the data returned by the first tasks matches the expected data
        with old_task.output().open() as json_input:
            lines = json_input.readlines()
            log.debug(lines)
            self.assertEquals(len(lines), 4)

        # Ensure the data returned by the two tasks is the same
        with new_task.output().open() as json_input:
            cache_lines = json_input.readlines()
            self.assertEquals(lines, cache_lines)


class TimestampPartitionMixinTest(CourseListTestMixin, TestCase):
    """Tests the TimestampPartitionMixin's formatted partition value."""

    task_class = CourseListPartitionTask
    timestamp = datetime.utcnow()
    partition_format = '%Y%m%dT%H%M%S'

    def create_task(self, **kwargs):
        """Create the task"""
        self.task = self.task_class(
            warehouse_path=self.output_dir,
            datetime=self.timestamp,
            **kwargs
        )

    def test_partition_value(self):
        """Ensure that partition value includes full datetime"""
        self.create_task(
            partition_format=self.partition_format,
        )
        expected_partition_value = self.timestamp.strftime(self.partition_format)
        self.assertEquals(self.task.partition_value, expected_partition_value)
