"""
Tests for encoding/decoding id values.
"""
from ddt import ddt, data, unpack

import edx.analytics.tasks.util.id_codec as id_codec
from edx.analytics.tasks.tests import unittest

SCOPE = "Arbitrary Scope"
TYPE = "Arbitrary Type"
VALUE = "Arbitrary Value"


@ddt
class EncodeDecodeIdTest(unittest.TestCase):
    """Test that encoding works in round-trip."""

    @data(
        '',
        '\u00e9',
        '\ufffd',
    )
    def test_round_trip(self, suffix):
        encoded_id = id_codec.encode_id(SCOPE + suffix, TYPE + suffix, VALUE + suffix)
        decoded = id_codec.decode_id(encoded_id)
        self.assertEquals((SCOPE + suffix, TYPE + suffix, VALUE + suffix), decoded)
