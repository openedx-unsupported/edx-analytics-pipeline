
import base64


def encode_id(scope, id_type, id_value):
    return base64.b32encode('|'.join([scope, id_type, id_value]))

def decode_id(encoded_id):
    scope, id_type, id_value = base64.b32decode(encoded_id).split('|')
    return scope, id_type, id_value
