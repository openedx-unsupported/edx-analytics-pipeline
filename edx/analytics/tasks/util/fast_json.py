"""
Provide an abstraction layer for fast json implementations across python 2 and 3.
"""
try:
    import ujson
    ujson_found = True
except ImportError:
    import cjson
    ujson_found = False

class FastJson(object):
    """
    Abstraction layer on top of cjson (python 2 only) and ujson (python 3 only).
    """
    @staticmethod
    def dumps(obj):
        """
        Dump/encode the Python object into a JSON message.
        """
        if ujson_found:
            return ujson.dumps(obj)
        else:
            return cjson.encode(obj)

    @staticmethod
    def loads(msg):
        """
        Load/decode the JSON message and return a Python object.

        All strings in the decoded object will be unicode strings!  This
        matches the behavior of python's built-in json library.
        """
        if ujson_found:
            return ujson.loads(msg)
        else:
            return cjson.decode(msg, all_unicode=True)
