"""
Provide an abstraction layer for fast json implementations across python 2 and 3.
"""
try:
    import ujson
except ImportError:
    import cjson

class FastJson(object):
    """
    Abstraction layer on top of cjson (python 2 only) and ujson (python 3 only).
    """
    @staticmethod
    def dumps(obj):
        if ujson:
            return ujson.dumps(obj)
        elif cjson:
            return cjson.encode(obj)

    @staticmethod
    def loads(msg):
        if ujson:
            return ujson.loads(obj)
        elif cjson:
            return cjson.decode(obj, all_unicode=True)
