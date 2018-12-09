"""
  Contains common methods for helping unit tests
"""

import logging
import os
import platform

log = logging.getLogger(__name__)


def is_mac():
    """
    A Mac's filesystem is case-insensitive. This method is used for
    skipping tests that expect a case-sensitive filesystem.
    """
    mac_ver_tuple = platform.mac_ver()
    if mac_ver_tuple[0]:
        return True


def load_fixture(filename):
    """
    Read the fixture file, and return its content.
    Returns None if unable to read the file.
    """
    content = None
    path = os.path.join(os.path.dirname(__file__), filename)
    with open(path) as input_file:
        content = input_file.read()
    return content
