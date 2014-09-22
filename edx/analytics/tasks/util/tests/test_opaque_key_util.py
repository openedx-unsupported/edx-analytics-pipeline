"""
Tests for utilities that parse event logs.
"""

from opaque_keys.edx.locator import CourseLocator

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.tests import unittest


VALID_COURSE_ID = unicode(CourseLocator(org='org', course='course_id', run='course_run'))
VALID_LEGACY_COURSE_ID = "org/course_id/course_run"
INVALID_LEGACY_COURSE_ID = "org:course_id:course_run"
NONASCII_LEGACY_COURSE_ID = u"org/course\ufffd_id/course_run"


class CourseIdTest(unittest.TestCase):
    """
    Verify that course_id filtering works correctly.
    """

    def test_normal_opaque_course_id(self):
        self.assertTrue(opaque_key_util.is_valid_course_id(VALID_COURSE_ID))

    def test_normal_legacy_course_id(self):
        self.assertTrue(opaque_key_util.is_valid_course_id(VALID_LEGACY_COURSE_ID))

    def test_legacy_course_id_without_components(self):
        self.assertFalse(opaque_key_util.is_valid_course_id(INVALID_LEGACY_COURSE_ID))

    def test_course_id_with_nonascii(self):
        self.assertFalse(opaque_key_util.is_valid_course_id(NONASCII_LEGACY_COURSE_ID))

    def test_no_course_id(self):
        self.assertFalse(opaque_key_util.is_valid_course_id(None))

    def test_valid_org_id(self):
        self.assertTrue(opaque_key_util.is_valid_org_id('org_id'))

    def test_invalid_org_id(self):
        self.assertFalse(opaque_key_util.is_valid_org_id(u'org\ufffd_id'))

    def test_no_org_id(self):
        self.assertFalse(opaque_key_util.is_valid_org_id(None))

    def test_get_valid_org_id(self):
        self.assertEquals(opaque_key_util.get_org_id_for_course(VALID_COURSE_ID), "org")

    def test_get_valid_legacy_org_id(self):
        self.assertEquals(opaque_key_util.get_org_id_for_course(VALID_LEGACY_COURSE_ID), "org")

    def test_get_invalid_legacy_org_id(self):
        self.assertIsNone(opaque_key_util.get_org_id_for_course(INVALID_LEGACY_COURSE_ID))
        self.assertIsNone(opaque_key_util.get_org_id_for_course(NONASCII_LEGACY_COURSE_ID))

    def test_get_filename(self):
        self.assertEquals(opaque_key_util.get_filename_safe_course_id(VALID_COURSE_ID), "org_course_id_course_run")
        self.assertEquals(opaque_key_util.get_filename_safe_course_id(VALID_COURSE_ID, '-'), "org-course_id-course_run")

    def test_get_filename_with_colon(self):
        course_id = unicode(CourseLocator(org='org', course='course:id', run='course:run'))
        self.assertEquals(opaque_key_util.get_filename_safe_course_id(VALID_COURSE_ID), "org_course_id_course_run")
        self.assertEquals(opaque_key_util.get_filename_safe_course_id(course_id, '-'), "org-course-id-course-run")

    def test_get_filename_for_legacy_id(self):
        self.assertEquals(
            opaque_key_util.get_filename_safe_course_id(VALID_LEGACY_COURSE_ID),
            "org_course_id_course_run"
        )
        self.assertEquals(
            opaque_key_util.get_filename_safe_course_id(VALID_LEGACY_COURSE_ID, '-'),
            "org-course_id-course_run"
        )

    def test_get_filename_for_invalid_id(self):
        self.assertEquals(
            opaque_key_util.get_filename_safe_course_id(INVALID_LEGACY_COURSE_ID),
            "org_course_id_course_run"
        )
        self.assertEquals(
            opaque_key_util.get_filename_safe_course_id(INVALID_LEGACY_COURSE_ID, '-'),
            "org-course_id-course_run"
        )

    def test_get_filename_for_nonascii_id(self):
        self.assertEquals(
            opaque_key_util.get_filename_safe_course_id(NONASCII_LEGACY_COURSE_ID),
            u"org_course\ufffd_id_course_run"
        )
        self.assertEquals(
            opaque_key_util.get_filename_safe_course_id(NONASCII_LEGACY_COURSE_ID, '-'),
            u"org-course\ufffd_id-course_run"
        )

    def test_get_course_key_from_url(self):
        url = "https://courses.edx.org/courses/{course_id}/stuff".format(course_id=VALID_COURSE_ID)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertEquals(unicode(course_key), VALID_COURSE_ID)

    def test_get_course_key_from_legacy_url(self):
        url = "https://courses.edx.org/courses/{course_id}/stuff".format(course_id=VALID_LEGACY_COURSE_ID)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertEquals(unicode(course_key), VALID_LEGACY_COURSE_ID)

    def test_get_course_key_from_invalid_url(self):
        url = "https://courses.edx.org/courses/{course_id}/stuff".format(course_id=INVALID_LEGACY_COURSE_ID)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertIsNone(course_key)

    def test_get_course_key_from_nonascii_url(self):
        url = u"https://courses.edx.org/courses/{course_id}/stuff".format(course_id=NONASCII_LEGACY_COURSE_ID)
        course_key = opaque_key_util.get_course_key_from_url(url)
        self.assertIsNone(course_key)
