"""Utility functions that wrap opaque_keys in useful ways."""

import re
import logging

from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from opaque_keys.edx.locator import CourseLocator


log = logging.getLogger(__name__)


# from lms/envs/common.py:
COURSE_KEY_PATTERN = r'(?P<course_key_string>[^/+]+(/|\+)[^/+]+(/|\+)[^/]+)'
COURSE_ID_PATTERN = COURSE_KEY_PATTERN.replace('course_key_string', 'course_id')
# from common/djangoapps/util/request.py:
COURSE_REGEX = re.compile(r'^.*?/courses/{}'.format(COURSE_ID_PATTERN))


def normalize_course_id(course_id):
    """Make a best effort to rescue malformed course_ids"""
    if course_id:
        return course_id.strip()
    else:
        return course_id


def is_valid_course_id(course_id):
    """
    Determines if a course_id from an event log is possibly legitimate.
    """
    if course_id and course_id[-1] == '\n':
        log.error("Found course_id that ends with a newline character '%s'", course_id)
        return False

    try:
        _course_key = CourseKey.from_string(course_id)
        return True
    except InvalidKeyError as exc:
        log.error("Unable to parse course_id '%s' : error = %s", course_id, exc)
        return False


def is_valid_org_id(org_id):
    """
    Determines if a course_id from an event log is possibly legitimate.
    """
    try:
        _course_key = CourseLocator(org=org_id, course="course", run="run")
        return True
    except InvalidKeyError as exc:
        log.error("Unable to parse org_id '%s' : error = %s", org_id, exc)
        return False


def get_org_id_for_course(course_id):
    """
    Args:
        course_id(unicode): The identifier for the course.

    Returns:
        The org_id extracted from the course_id, or None if none is found.
    """

    try:
        course_key = CourseKey.from_string(course_id)
        return course_key.org
    except InvalidKeyError:
        return None


def get_filename_safe_course_id(course_id, replacement_char='_'):
    """
    Create a representation of a course_id that can be used safely in a filepath.
    """
    try:
        course_key = CourseKey.from_string(course_id)
        # Ignore the namespace of the course_id altogether, for backwards compatibility.
        filename = course_key._to_string()  # pylint: disable=protected-access
    except InvalidKeyError:
        # If the course_id doesn't parse, we will still return a value here.
        filename = course_id

    # The safest characters are A-Z, a-z, 0-9, <underscore>, <period> and <hyphen>.
    # We represent the first four with \w.
    # TODO: Once we support courses with unicode characters, we will need to revisit this.
    return re.sub(r'[^\w\.\-]', unicode(replacement_char), filename)


def get_course_key_from_url(url):
    """
    Extracts the course from the given `url`, if possible.

    """
    url = url or ''

    match = COURSE_REGEX.match(url)
    course_key = None
    if match:
        course_id_string = match.group('course_id')
        try:
            course_key = CourseKey.from_string(course_id_string)
        except InvalidKeyError:
            pass

    return course_key
