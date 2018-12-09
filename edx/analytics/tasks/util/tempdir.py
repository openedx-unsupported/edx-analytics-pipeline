"""Utility methods to help with temp-file management."""
import atexit
import os.path
import shutil
import tempfile
from contextlib import contextmanager


@contextmanager
def make_temp_directory(suffix='', prefix='tmp', dir=None):  # pylint: disable=redefined-builtin
    """Creates a temp directory and makes sure to clean it up."""
    # create secure temp directory
    temp_dir = tempfile.mkdtemp(suffix, prefix, dir)

    # and delete it at exit
    def clean_dir():
        """Delete the temp directory."""
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    # make sure the directory is deleted, even if interrupted
    atexit.register(clean_dir)

    try:
        yield temp_dir
    finally:
        clean_dir()
