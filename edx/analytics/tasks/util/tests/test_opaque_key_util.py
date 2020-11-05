"""
Tests for utilities that parse event logs.
"""
from unittest import TestCase

from ccx_keys.locator import CCXLocator
from ddt import data, ddt, unpack
from opaque_keys.edx.locator import BlockUsageLocator, CourseLocator

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util

VALID_COURSE_KEY = CourseLocator(org='org', course='course_id', run='course_run')
VALID_COURSE_ID = unicode(VALID_COURSE_KEY)
VALID_LEGACY_COURSE_ID = "org/course_id/course_run"
INVALID_LEGACY_COURSE_ID = "org:course_id:course_run"
INVALID_NONASCII_LEGACY_COURSE_ID = u"org/course\ufffd_id/course_run"
VALID_NONASCII_LEGACY_COURSE_ID = u"org/cours\u00e9_id/course_run"
VALID_CCX_COURSE_ID = unicode(CCXLocator(org='org', course='course_id', run='course_run', ccx='13'))
COURSE_ID_WITH_COLONS = unicode(CourseLocator(org='org', course='course:id', run='course:run'))
VALID_BLOCK_ID = BlockUsageLocator(course_key=VALID_COURSE_KEY, block_type='video', block_id='Welcome')


@ddt
class CourseIdTest(TestCase):
    """
    Verify that course_id filtering works correctly.
    """

    @data(
        VALID_COURSE_ID,
        VALID_LEGACY_COURSE_ID,
        VALID_NONASCII_LEGACY_COURSE_ID,
        VALID_CCX_COURSE_ID,
    )
    def test_valid_course_id(self, course_id):
        self.assertTrue(opaque_key_util.is_valid_course_id(course_id))

    @data(
        INVALID_LEGACY_COURSE_ID,
        INVALID_NONASCII_LEGACY_COURSE_ID,
        None,
        VALID_COURSE_ID + '\n',
        '',
        '\n',
    )
    def test_invalid_course_id(self, course_id):
        self.assertFalse(opaque_key_util.is_valid_course_id(course_id))

    @data(
        u'org_id\u00e9',
    )
    def test_valid_org_id(self, org_id):
        self.assertTrue(opaque_key_util.is_valid_org_id(org_id))

    @data(
        u'org\ufffd_id',
        None,
    )
    def test_invalid_org_id(self, org_id):
        self.assertFalse(opaque_key_util.is_valid_org_id(org_id))

    @data(
        VALID_COURSE_ID,
        VALID_LEGACY_COURSE_ID,
        VALID_NONASCII_LEGACY_COURSE_ID,
        VALID_CCX_COURSE_ID,
    )
    def test_get_valid_org_id(self, course_id):
        self.assertEquals(opaque_key_util.get_org_id_for_course(course_id), "org")

    @data(
        INVALID_LEGACY_COURSE_ID,
        INVALID_NONASCII_LEGACY_COURSE_ID,
        None,
    )
    def test_get_invalid_org_id(self, course_id):
        self.assertIsNone(opaque_key_util.get_org_id_for_course(course_id))

    @data(
        (VALID_COURSE_ID, "org_course_id_course_run", "org-course_id-course_run"),
        (COURSE_ID_WITH_COLONS, "org_course_id_course_run", "org-course-id-course-run"),
        (VALID_LEGACY_COURSE_ID, "org_course_id_course_run", "org-course_id-course_run"),
        (INVALID_LEGACY_COURSE_ID, "org_course_id_course_run", "org-course_id-course_run"),
        (VALID_NONASCII_LEGACY_COURSE_ID, u"org_cours__id_course_run", u"org-cours-_id-course_run"),
        (INVALID_NONASCII_LEGACY_COURSE_ID, u"org_course__id_course_run", u"org-course-_id-course_run"),
        (VALID_CCX_COURSE_ID, "org_course_id_course_run_ccx_13", "org-course_id-course_run-ccx-13"),
    )
    @unpack
    def test_get_filename_with_default_separator(self, course_id, expected_filename, expected_filename_with_hyphen):
        self.assertEquals(opaque_key_util.get_filename_safe_course_id(course_id), expected_filename)
        self.assertEquals(opaque_key_util.get_filename_safe_course_id(course_id, '-'), expected_filename_with_hyphen)

    @data(
        VALID_COURSE_ID,
        VALID_LEGACY_COURSE_ID,
        VALID_NONASCII_LEGACY_COURSE_ID,
        VALID_CCX_COURSE_ID,
    )
    def test_get_course_key_from_url(self, course_id):
        url = u"https://courses.edx.org/courses/{course_id}/stuff".format(course_id=course_id)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertEquals(unicode(course_key), course_id)

    @data(
        VALID_BLOCK_ID,
    )
    def test_get_course_key_from_url(self, block_id):
        url = u"https://courses.edx.org/xblock/{block_id}?stuff=things".format(block_id=block_id)
        print(url)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertEquals(unicode(course_key), VALID_COURSE_ID)

    @data(
        INVALID_LEGACY_COURSE_ID,
        INVALID_NONASCII_LEGACY_COURSE_ID,
        None,
        '',
        '\n',
    )
    def test_get_course_key_from_invalid_url(self, course_id):
        url = u"https://courses.edx.org/courses/{course_id}/stuff".format(course_id=course_id)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertIsNone(course_key)

    @data(
        None,
        '',
        'Invalid+Block+ID',
        '\n',
    )
    def test_get_course_key_from_invalid_block_url(self, block_id):
        url = u"https://courses.edx.org/xblock/{block_id}?stuff=things".format(block_id=block_id)
        print(url)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertIsNone(course_key)
