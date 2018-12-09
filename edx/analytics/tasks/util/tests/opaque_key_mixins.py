"""Mixin classes for providing opaque or legacy key values."""

from opaque_keys.edx.locator import CourseLocator


class InitializeOpaqueKeysMixin(object):
    """Test class for providing common opaque key values for tests."""

    def initialize_ids(self):
        """Define set of id values for use in tests."""
        course_key = CourseLocator(org=u'FooX\u00e9', course='1.23x', run='2013_Spring')
        self.course_id = unicode(course_key)
        self.encoded_course_id = self.course_id.encode('utf8')
        self.org_id = course_key.org
        self.encoded_org_id = self.org_id.encode('utf8')

        block_id = "9cee77a606ea4c1aa5440e0ea5d0f618"
        self.problem_id = unicode(course_key.make_usage_key("problem", block_id))
        self.encoded_problem_id = self.problem_id.encode('utf8')
        self.answer_id = "{block_id}_2_1".format(block_id=block_id)
        self.second_answer_id = "{block_id}_3_1".format(block_id=block_id)

    def empty_ids(self):
        """Set keys to empty strings."""
        self.course_id = ""
        self.encoded_course_id = ""
        self.org_id = ""
        self.encoded_org_id = ""
        self.problem_id = ""
        self.encoded_problem_id = ""
        self.second_answer_id = ""
        self.answer_id = ""
        self.user_id = ""


class InitializeLegacyKeysMixin(object):
    """Test class for providing common legacy key values for tests."""

    def initialize_ids(self):
        """Define set of id values for use in tests."""
        self.course_id = "FooX/1.23x/2013_Spring"
        self.encoded_course_id = self.course_id.encode('utf8')
        self.org_id = self.course_id.split('/')[0]
        self.encoded_org_id = self.org_id.encode('utf8')
        self.problem_id = "i4x://FooX/1.23x/2013_Spring/problem/PSet1:PS1_Q1"
        self.encoded_problem_id = self.problem_id.encode('utf8')
        self.answer_id = "i4x-FooX-1_23x-problem-PSet1_PS1_Q1_2_1"
        self.second_answer_id = "i4x-FooX-1_23x-problem-PSet1_PS1_Q1_3_1"
