"""Tests for deidentification utilities."""

from ddt import data, ddt

from edx.analytics.tasks.tests import unittest
import edx.analytics.tasks.util.deid_util as deid_util


@ddt
class BackslashHandlingTestCase(unittest.TestCase):
    """Test encoding and decoding of backslashed data."""

    @data(
        'This is a test.\\nThis is another.',
    )
    def test_needs_backslash_decoding(self, text):
        self.assertTrue(deid_util.needs_backslash_decoding(text))

    @data(
        'This is a test.\\nThis is another.\n',
        'This is a test.',
    )
    def test_needs_no_backslash_decoding(self, text):
        self.assertFalse(deid_util.needs_backslash_decoding(text))
