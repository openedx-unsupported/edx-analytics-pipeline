"""Utility functions that wrap opaque_keys in useful ways."""

import logging
import re

from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from opaque_keys.edx.locator import BlockUsageLocator, CourseLocator

log = logging.getLogger(__name__)

# Regex to extract a course_id from a course URL.
# from openedx/core/constants.py:
COURSE_KEY_PATTERN = r'(?P<course_key_string>[^/+]+(/|\+)[^/+]+(/|\+)[^/?]+)'
COURSE_ID_PATTERN = COURSE_KEY_PATTERN.replace('course_key_string', 'course_id')
# from openedx/core/lib/request_utils.py:
COURSE_REGEX = re.compile(r'^(.*?/courses/)(?!v[0-9]+/[^/]+){}'.format(COURSE_ID_PATTERN))

# Regex to extract a course_id from a block URL/block ID.
BLOCK_ID_PATTERN = r'(?P<block_id>[^/+]+(/|\+)[^/+]+(/|\+)[^/?#]+)'
BLOCK_REGEX = re.compile(r'^(.*?/xblock/){}'.format(BLOCK_ID_PATTERN))

# Make sure that Opaque Keys' Stevedore extensions are loaded, this can sometimes fail to happen in EMR
# and it is vitally important that they be there, or jobs will succeed but produce incorrect data. See
# https://openedx.atlassian.net/wiki/spaces/DE/pages/1934263829/RCA+Insights+data+issues+2020-10-14+-+2020-10-15
# This will raise an error early on in import if the plugins don't exist.
CourseKey.get_namespace_plugin('course-v1')


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

    # First, try to extract the course_id assuming the URL follows this pattern:
    # https://courses.edx.org/xblock/block-v1:org+course+run+type@vertical+block@3848270e75f34e409eaad53a2a7f1da5?show_title=0&show_bookmark_button=0
    block_match = BLOCK_REGEX.match(url)
    if block_match:
        block_id_string = block_match.group('block_id')
        try:
            return BlockUsageLocator.from_string(block_id_string).course_key
        except InvalidKeyError:
            return None

    # Second, try to extract the course_id assuming the URL follows this pattern:
    # https://courses.edx.org/courses/course-v1:org+course+run/courseware/unit1/der_3-sequential/?activate_block_id=block-v1%3Aorg%2Bcourse%2Brun%2Btype%40sequential%2Bblock%40der_3-sequential
    course_match = COURSE_REGEX.match(url)
    if course_match:
        course_id_string = course_match.group('course_id')
        try:
            return CourseKey.from_string(course_id_string)
        except InvalidKeyError:
            return None
