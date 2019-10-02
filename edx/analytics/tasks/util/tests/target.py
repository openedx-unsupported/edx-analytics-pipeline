"""
Emulates a luigi target, storing all data in memory.
"""
from __future__ import absolute_import

from contextlib import contextmanager
from io import BytesIO
import six


class FakeTarget(object):
    """
    Fake Luigi-like target that saves data in memory, using a BytesIO buffer.
    """
    def __init__(self, path=None, value=b''):
        self.value = value
        self.path = path

    @property
    def value(self):
        return self.buffer.getvalue()

    @value.setter
    def value(self, value):
        self.buffer = BytesIO(value)
        # Rewind the buffer head so the value can be read
        self.buffer.seek(0)

    @contextmanager
    def open(self, *args, **kwargs):  # pylint: disable=unused-argument
        """
        Returns:
            A file-like object that can be used to read the data that is stored in the buffer.
        """
        try:
            yield self.buffer
        finally:
            self.buffer.seek(0)

    def exists(self):
        return len(self.value) > 0


class FakeTask(object):
    """Fake Luigi-like task that wraps a FakeTarget."""
    def __init__(self, path=None, value=''):
        self.target = FakeTarget(path=path, value=value)

    def output(self):
        """Return FakeTarget for use in tests."""
        return self.target
