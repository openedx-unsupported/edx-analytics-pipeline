"""Defines a reversible way to label an id with a scope and type to make it unique."""

import base64


def encode_id(scope, id_type, id_value):
    """Encode a scope-type-value tuple into a single ID string."""
    return base64.b32encode('|'.join([scope, id_type, id_value]))


def decode_id(encoded_id):
    """Decode an ID string back to the original scope-type-value tuple."""
    scope, id_type, id_value = base64.b32decode(encoded_id).split('|')
    return scope, id_type, id_value
