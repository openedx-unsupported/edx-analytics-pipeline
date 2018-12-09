"""Ensure manifest files are created appropriately."""

import os
import shutil
import tempfile
from unittest import TestCase

import luigi
from luigi.contrib.hdfs.target import HdfsTarget
from mock import patch

from edx.analytics.tasks.util.manifest import (
    ManifestInputTargetMixin, convert_to_manifest_input_if_necessary, create_manifest_target,
    remove_manifest_target_if_exists
)
from edx.analytics.tasks.util.tests.config import OPTION_REMOVED, with_luigi_config
from edx.analytics.tasks.util.tests.target import FakeTarget


class ManifestInputTargetTest(TestCase):
    """Ensure manifest files are created appropriately."""

    MANIFEST_ID = 'test'

    def setUp(self):
        patcher = patch('edx.analytics.tasks.util.manifest.get_target_class_from_url')
        self.get_target_class_from_url_mock = patcher.start()
        self.addCleanup(patcher.stop)
        self.get_target_class_from_url_mock.side_effect = lambda url: (FakeTarget, (url,), {})

    @with_luigi_config(
        ('manifest', 'path', '/tmp/manifest'),
        ('manifest', 'lib_jar', '/tmp/foo.jar'),
        ('manifest', 'input_format', 'com.example.SimpleFormat')
    )
    def test_annotate_output_target(self):
        target = create_manifest_target(self.MANIFEST_ID, [luigi.LocalTarget('/tmp/foo')])

        self.assertEquals(target.path, '/tmp/manifest/test.manifest')
        self.assertEquals(target.lib_jar, ['/tmp/foo.jar'])
        self.assertEquals(target.input_format, 'com.example.SimpleFormat')

    @with_luigi_config(
        ('manifest', 'path', '/tmp/manifest'),
        ('manifest', 'lib_jar', OPTION_REMOVED),
        ('manifest', 'input_format', OPTION_REMOVED)
    )
    def test_parameters_not_configured(self):
        target = create_manifest_target(self.MANIFEST_ID, [luigi.LocalTarget('/tmp/foo')])

        self.assertEquals(target.path, '/tmp/manifest/test.manifest')
        self.assertFalse(hasattr(target, 'lib_jar'))
        self.assertFalse(hasattr(target, 'input_format'))

    def test_manifest_file_construction(self):
        target = create_manifest_target(self.MANIFEST_ID, [HdfsTarget('s3://foo/bar')])
        self.assertEquals(target.value, 's3://foo/bar\n')

    @with_luigi_config('manifest', 'threshold', 1)
    def test_over_threshold(self):
        targets = convert_to_manifest_input_if_necessary(self.MANIFEST_ID, [
            luigi.LocalTarget('/tmp/foo'),
            luigi.LocalTarget('/tmp/foo2')
        ])

        self.assertEquals(len(targets), 1)
        self.assertIsInstance(targets[0], ManifestInputTargetMixin)

    @with_luigi_config('manifest', 'threshold', 3)
    def test_under_threshold(self):
        self.assert_no_conversion()

    def assert_no_conversion(self):
        original_targets = [
            luigi.LocalTarget('/tmp/foo'),
            luigi.LocalTarget('/tmp/foo2')
        ]
        targets = convert_to_manifest_input_if_necessary(self.MANIFEST_ID, original_targets)
        self.assertEquals(original_targets, targets)

    @with_luigi_config('manifest', 'threshold', -1)
    def test_negative_threshold(self):
        self.assert_no_conversion()

    @with_luigi_config('manifest', 'threshold', OPTION_REMOVED)
    def test_threshold_not_set(self):
        self.assert_no_conversion()


temp_rootdir = None


class ManifestInputTargetDeletionTest(TestCase):
    """Ensure manifest files are deleted appropriately."""

    MANIFEST_ID = 'test'

    def setUp(self):
        temp_rootdir = tempfile.mkdtemp()
        self.addCleanup(self.cleanup, temp_rootdir)

    def cleanup(self, dirname):
        """Remove the temp directory only if it exists."""
        if os.path.exists(dirname):
            shutil.rmtree(dirname)

    @with_luigi_config(
        ('manifest', 'path', temp_rootdir),
    )
    def test_manifest_file_deletion(self):
        target = create_manifest_target(self.MANIFEST_ID, [HdfsTarget('s3://foo/bar')])
        self.assertTrue(target.exists())
        remove_manifest_target_if_exists(self.MANIFEST_ID)
        self.assertFalse(target.exists())
