"""Tests for classes defined in mapreduce.py."""

from __future__ import absolute_import

import json
import os
import shutil
import tempfile
import unittest

import luigi
import luigi.hdfs
from mock import patch, call

from edx.analytics.tasks.common.mapreduce import MultiOutputMapReduceJobTask, MapReduceJobTask


class MapReduceJobTaskTest(unittest.TestCase):
    """Tests for MapReduceJobTask"""

    def test_job_with_special_input_targets(self):
        lib_jar_path = ['hdfs:///tmp/something.jar']
        input_format = 'com.example.SpecialInputFormat'

        required_job = TaskWithSpecialOutputs(lib_jar_path=lib_jar_path, input_format=input_format)
        job = DynamicRequirementsJob()
        job.requirements = [required_job]

        runner = job.job_runner()

        self.assertItemsEqual(runner.libjars_in_hdfs, lib_jar_path)
        self.assertEquals(runner.input_format, input_format)

    def test_job_with_different_input_formats(self):
        job = DynamicRequirementsJob()
        job.requirements = [
            TaskWithSpecialOutputs(input_format='foo'),
            TaskWithSpecialOutputs(input_format='bar')
        ]

        with self.assertRaises(RuntimeError):
            job.job_runner()

    def test_multiple_lib_jars(self):
        job = DynamicRequirementsJob()
        job.requirements = [
            TaskWithSpecialOutputs(lib_jar_path=['foo', 'bar']),
            TaskWithSpecialOutputs(lib_jar_path=['baz'])
        ]

        runner = job.job_runner()

        self.assertItemsEqual(runner.libjars_in_hdfs, ['foo', 'bar', 'baz'])

    def test_missing_input_format(self):
        job = DynamicRequirementsJob()
        job.requirements = [
            TaskWithSpecialOutputs(lib_jar_path=['foo'], input_format='com.example.Foo'),
            TaskWithSpecialOutputs(lib_jar_path=['baz'])
        ]

        runner = job.job_runner()

        self.assertItemsEqual(runner.libjars_in_hdfs, ['foo', 'baz'])
        self.assertEquals(runner.input_format, 'com.example.Foo')


class TaskWithSpecialOutputs(luigi.ExternalTask):
    """A task with a single output that requires the use of a configurable library jar and input format."""

    lib_jar_path = luigi.ListParameter(default=[])
    input_format = luigi.Parameter(default=None)

    def output(self):
        target = luigi.hdfs.HdfsTarget('/tmp/foo')
        target.lib_jar = self.lib_jar_path
        target.input_format = self.input_format
        return target


class DynamicRequirementsJob(MapReduceJobTask):
    """A task with configurable requirements."""

    def requires(self):
        return self.requirements


class MultiOutputMapReduceJobTaskTest(unittest.TestCase):
    """Tests for MultiOutputMapReduceJobTask."""

    def setUp(self):
        patcher = patch('edx.analytics.tasks.common.mapreduce.get_target_from_url')
        self.mock_get_target = patcher.start()
        self.addCleanup(patcher.stop)

        self.task = TestJobTask(
            mapreduce_engine='local',
            output_root='/any/path',
        )

    def test_reducer(self):
        self.assert_values_written_to_file('foo', ['bar', 'baz'])

    def assert_values_written_to_file(self, key, values):
        """Confirm that values passed to reducer appear in output file."""
        self.assertItemsEqual(self.task.reducer(key, values), [])

        self.mock_get_target.assert_called_once_with('/any/path/' + key)

        mock_target = self.mock_get_target.return_value
        mock_file = mock_target.open.return_value.__enter__.return_value
        mock_file.write.assert_has_calls([call(v + '\n') for v in values])

        self.mock_get_target.reset_mock()

    def test_multiple_reducer_calls(self):
        self.assert_values_written_to_file('foo', ['bar', 'baz'])
        self.assert_values_written_to_file('foo2', ['bar2'])


class MultiOutputMapReduceJobTaskOutputRootTest(unittest.TestCase):
    """Tests for output_root behavior of MultiOutputMapReduceJobTask."""

    def setUp(self):
        # Define a real output directory, so it can
        # be removed if existing.
        def cleanup(dirname):
            """Remove the temp directory only if it exists."""
            if os.path.exists(dirname):
                shutil.rmtree(dirname)

        self.output_root = tempfile.mkdtemp()
        self.addCleanup(cleanup, self.output_root)

        patcher = patch('edx.analytics.tasks.common.mapreduce.luigi.configuration.get_config')
        self.mock_get_config = patcher.start()
        self.addCleanup(patcher.stop)

    def DONT_tesst_no_delete_output_root(self):
        # TODO: fix or remove this.
        self.assertTrue(os.path.exists(self.output_root))
        TestJobTask(
            mapreduce_engine='local',
            output_root=self.output_root,
        )
        self.assertTrue(os.path.exists(self.output_root))

    def DONT_tesst_delete_output_root(self):
        # TODO: fix or remove this.
        temporary_file_path = tempfile.mkdtemp()
        # self.mock_get_config.return_value.get.return_value = temporary_file_path
        # This should be only set for the parameter that needs it,
        # not for all values, because all will be called with get(),
        # and the one value won't work.
        self.mock_get_config.return_value.get.return_value = json.dumps([temporary_file_path,])
        self.addCleanup(shutil.rmtree, temporary_file_path)

        # We create a task in order to get the output path.
        task = TestJobTask(
            mapreduce_engine='local',
            output_root=self.output_root,
        )
        output_marker = task.output().path
        open(output_marker, 'a').close()
        self.assertTrue(task.complete())

        # Once the output path is created, we can
        # then confirm that it gets cleaned up..
        task = TestJobTask(
            mapreduce_engine='local',
            output_root=self.output_root,
            delete_output_root="true",
        )
        self.assertFalse(task.complete())
        self.assertFalse(os.path.exists(self.output_root))


class TestJobTask(MultiOutputMapReduceJobTask):
    """Dummy task to use for testing."""

    def output_path_for_key(self, key):
        return os.path.join(self.output_root, key)

    def multi_output_reducer(self, _key, values, output_file):
        for value in values:
            output_file.write(value + '\n')
