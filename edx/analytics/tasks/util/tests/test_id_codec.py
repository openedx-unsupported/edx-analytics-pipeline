"""
Tests for encoding/decoding id values.
"""
from __future__ import absolute_import

from unittest import TestCase

from ddt import data, ddt

import edx.analytics.tasks.util.id_codec as id_codec

SCOPE = b"Arbitrary Scope"
TYPE = b"Arbitrary Type"
VALUE = b"Arbitrary Value"


@ddt
class EncodeDecodeIdTest(TestCase):
    """Test that encoding works in round-trip."""

    @data(
        b'',
        b'test',
        u'\ufffd'.encode('utf8'),
        u'\u00e9'.encode('utf8'),
    )
    def test_round_trip(self, suffix):
        input_id = (SCOPE + suffix, TYPE + suffix, VALUE + suffix)
        decoded_id = id_codec.decode_id(id_codec.encode_id(*input_id))
        self.assertEquals(input_id, decoded_id)


class PermutationGeneratorTest(TestCase):
    """Test that PermutationGenerator works correctly."""

    def test_permute_unpermute(self):
        id_value = 123456
        permutation_generator = id_codec.PermutationGenerator(42, 32, 32)

        permuted = permutation_generator.permute(id_value)
        self.assertEquals(permuted, 273678626)

        unpermuted = permutation_generator.unpermute(permuted)
        self.assertEquals(unpermuted, id_value)
