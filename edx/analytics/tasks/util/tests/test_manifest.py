"""Ensure manifest files are created appropriately."""

import luigi
from mock import patch

from edx.analytics.tasks.url import UncheckedExternalURL
from edx.analytics.tasks.util.manifest import URLManifestTask, convert_tasks_to_manifest_if_necessary
from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks.tests.config import with_luigi_config, OPTION_REMOVED
from edx.analytics.tasks.tests.target import FakeTarget


class URLManifestTaskTest(unittest.TestCase):
    """Ensure manifest files are created appropriately."""

    SOURCE_URL = 's3://foo/bar'
    MANIFEST_BASE_PATH = '/tmp/manifest'

    def setUp(self):
        self.task = URLManifestTask(urls=[self.SOURCE_URL])
        self.expected_path = '{0}/{1}.manifest'.format(self.MANIFEST_BASE_PATH, hash(self.task))

    @with_luigi_config(
        ('manifest', 'path', MANIFEST_BASE_PATH),
        ('manifest', 'lib_jar', '/tmp/foo.jar'),
        ('manifest', 'input_format', 'com.example.SimpleFormat')
    )
    def test_annotate_output_target(self):
        target = self.task.output()

        self.assertEquals(target.path, self.expected_path)
        self.assertEquals(target.lib_jar, ['/tmp/foo.jar'])
        self.assertEquals(target.input_format, 'com.example.SimpleFormat')

    @with_luigi_config(
        ('manifest', 'path', MANIFEST_BASE_PATH),
        ('manifest', 'lib_jar', OPTION_REMOVED),
        ('manifest', 'input_format', OPTION_REMOVED)
    )
    def test_parameters_not_configured(self):
        target = self.task.output()

        self.assertEquals(target.path, self.expected_path)
        self.assertFalse(hasattr(target, 'lib_jar'))
        self.assertFalse(hasattr(target, 'input_format'))

    @patch('edx.analytics.tasks.util.manifest.get_target_from_url')
    def test_manifest_file_construction(self, get_target_from_url_mock):
        fake_target = FakeTarget()
        get_target_from_url_mock.return_value = fake_target

        self.task.run()

        content = fake_target.buffer.read()
        self.assertEquals(content, self.SOURCE_URL + '\n')

    def test_requirements(self):
        self.assertItemsEqual(self.task.requires(), [UncheckedExternalURL(self.SOURCE_URL)])


class ConversionTest(unittest.TestCase):
    """Ensure large numbers of inputs are correctly converted into manifest tasks when appropriate."""

    @with_luigi_config('manifest', 'threshold', 1)
    def test_over_threshold(self):
        tasks = convert_tasks_to_manifest_if_necessary([FakeTask(), FakeTask()])

        self.assertEquals(len(tasks), 1)
        self.assertIsInstance(tasks[0], URLManifestTask)

    @with_luigi_config('manifest', 'threshold', 3)
    def test_under_threshold(self):
        tasks = convert_tasks_to_manifest_if_necessary([FakeTask(), FakeTask()])

        self.assertEquals(len(tasks), 2)
        self.assertIsInstance(tasks[0], FakeTask)
        self.assertIsInstance(tasks[1], FakeTask)

    @with_luigi_config('manifest', 'threshold', 2)
    def test_task_with_many_targets(self):
        class MultiTargetTask(luigi.ExternalTask):
            """A fake task with multiple outputs."""
            def output(self):
                return [
                    luigi.LocalTarget('/tmp/foo'),
                    luigi.LocalTarget('/tmp/bar'),
                ]

        tasks = convert_tasks_to_manifest_if_necessary(MultiTargetTask())

        self.assertEquals(len(tasks), 1)
        self.assertIsInstance(tasks[0], URLManifestTask)

    @with_luigi_config('manifest', 'threshold', -1)
    def test_negative_threshold(self):
        tasks = convert_tasks_to_manifest_if_necessary([FakeTask(), FakeTask()])

        self.assertEquals(len(tasks), 2)
        self.assertIsInstance(tasks[0], FakeTask)
        self.assertIsInstance(tasks[1], FakeTask)

    @with_luigi_config('manifest', 'threshold', OPTION_REMOVED)
    def test_threshold_not_set(self):
        tasks = convert_tasks_to_manifest_if_necessary([FakeTask(), FakeTask()])

        self.assertEquals(len(tasks), 2)
        self.assertIsInstance(tasks[0], FakeTask)
        self.assertIsInstance(tasks[1], FakeTask)


class FakeTask(luigi.ExternalTask):
    """A fake task with a single output target."""
    def output(self):
        return luigi.LocalTarget('/tmp/foo')
