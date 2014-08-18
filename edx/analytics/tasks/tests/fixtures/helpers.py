"""
  Contains common methods for helping unit tests
"""

import platform


def is_mac():
    """
    A Mac's filesystem is case-insensitive. This method is used for
    skipping tests that expect a case-sensitive filesystem.
    """
    mac_ver_tuple = platform.mac_ver()
    if mac_ver_tuple[0]:
        return True
