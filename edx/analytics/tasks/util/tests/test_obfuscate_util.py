# -*- coding: utf-8 -*-
"""Tests for obfuscation utilities."""

import textwrap
from unittest import TestCase

from ddt import data, ddt, unpack
from mock import MagicMock, patch

import edx.analytics.tasks.util.obfuscate_util as obfuscate_util
from edx.analytics.tasks.util.tests.target import FakeTask


@ddt
class BackslashHandlingTestCase(TestCase):
    """Test encoding and decoding of backslashed data."""

    @data(
        'This is a test.\\nThis is another.',
    )
    def test_needs_backslash_decoding(self, text):
        self.assertTrue(obfuscate_util.needs_backslash_decoding(text))

    @data(
        'This is a test.\\nThis is another.\n',
        'This is a test.',
    )
    def test_needs_no_backslash_decoding(self, text):
        self.assertFalse(obfuscate_util.needs_backslash_decoding(text))

    @data(
        'This is a test.\\nThis is another.',
        # 'This is a test.\\\nThis is another.',
        'This is a test.\\\\\\nThis is another.',
        u'This is a test.\\nThis is another.',
        u'This is a test.\\\\\\nThis is another.',
        u'This is a \u00e9 test.\\\\nThis is another.',
    )
    def test_decoding_round_trip(self, text):
        self.assertEquals(text, obfuscate_util.backslash_encode_value(obfuscate_util.backslash_decode_value(text)))

    @data(
        'This is a test.\\nThis is another.',
        'This is a test.\\\nThis is another.',
        'This is a test.\\\\\\nThis is another.',
        u'This is a test.\\nThis is another.',
        u'This is a test.\\\\\\nThis is another.',
        u'This is a \u00e9 test.\\\\nThis is another.',
    )
    def test_encoding_round_trip(self, text):
        self.assertEquals(text, obfuscate_util.backslash_decode_value(obfuscate_util.backslash_encode_value(text)))

    @data(
        ('Test1\nTest2', 'Test1\\nTest2'),
        ('Test1\\nTest2', 'Test1\\\\nTest2'),
        ('Test1\\\nTest2', 'Test1\\\\\\nTest2'),
    )
    @unpack
    def test_encoding(self, text, expected_result):
        self.assertEquals(obfuscate_util.backslash_encode_value(text), expected_result)

    @data(
        ('Test1\\nTest2', 'Test1\nTest2'),
        ('Test1\\\nTest2', 'Test1\\\nTest2'),
        ('Test1\\\\nTest2', 'Test1\\nTest2'),
        ('Test1\\\\\nTest2', 'Test1\\\nTest2'),
        ('Test1\\\\\\nTest2', 'Test1\\\nTest2'),
    )
    @unpack
    def test_decoding(self, text, expected_result):
        self.assertEquals(obfuscate_util.backslash_decode_value(text), expected_result)


@ddt
class FindMatchesTestCase(TestCase):
    """Test finding matches for regular expressions in strings."""

    SIMPLE_CONTEXT = u"This is left context: {} This is right context."

    #####################
    # phone
    #####################

    @data(
        '555-1212',
        '555 1212',
        '555 -  1212',
        '555  1212',
        '1-201-555-1212',
        '201-555-1212',
        '+91 12 1234 1234',
        '+91-123-123456',
        '+46-123 123 123',
        '+57 123 123-4567',
        '+919876543210',
        '(202) 123-4567',
        '(202)123-4567',
        '+98-123-1234567',
        '+91-8765432101',
        '+63 123 123 12 12',
        '+63 1 123 1234',
        '1 849 123 1234',
        # These are failing...
        # '818.123.1234',
        # '(04) 123 1234',
        # '0300 123 1234',
        # '0800 123 123',
        # '0800 123 1234',
        # '010-5432-1234',
    )
    def test_find_simple_phone_numbers(self, text):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format("<<PHONE_NUMBER>>")
        result = obfuscate_util.find_phone_numbers(raw)
        self.assertEquals(expected, result)

    @data(
        # These overflags need to be fixed:
        # 'http://link.springer.com/article/10.1007%2Fs13524-014-0321-x',
        # u' Mindfulness, 1 \\u2013 8. DOI 10.1007/s12671-015-0408-5\nMarlatt, A. G., & Donavon, D. M. (2005)',
        # 'ISBN 0-813 2410-7',
        # 'maybe 1,000 - 1100 words.',
        # 'www.digikey.com/product-detail/en/AST-030C0MR-R/668-1138-ND/1464877',
        # '450-1650-ND',
        '123.4567',
    )
    def test_skip_non_phone_numbers(self, text):
        raw = self.SIMPLE_CONTEXT.format(text)
        result = obfuscate_util.find_phone_numbers(raw)
        self.assertEquals(raw, result)

    @data(
        ('Cell #:240-123-4567', 'Cell #:<<PHONE_NUMBER>>'),
    )
    @unpack
    def test_find_phone_numbers_in_context(self, text, result):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format(result)
        actual = obfuscate_util.find_phone_numbers(raw)
        self.assertEquals(expected, actual)

    #####################
    # email
    #####################

    @data(
        'testuser@gmail.com',
        'test.user@gmail.com',
        'test_user@gmail.com',
        'Ttestuser@gmail.com',
        'test.user+foo@gmail.com',
        'email1234-yes@yahoo.de',
        'info@edx.org',
        'Testers@yahoo.com',
        'a.tester@hotmail.co.uk',
        'a-person@a-domain.de',
        'aperson@yahoo.co.in',
        'aperson21@hotmail.com',
        'aperson.cs@abc-def.edu.pk',
        '123456789@example.com',
        'this_is_a_test@example.com',
        't0e1s2t3@test.abc.dk',
        'First.Last@example.co.uk',
    )
    def test_find_simple_emails(self, text):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format("<<EMAIL>>")
        result = obfuscate_util.find_emails(raw)
        self.assertEquals(expected, result)

    @data(
        'Twitter at @username.',
        # These overflags need to be fixed:
        #  edx.org/asset-v1:edX+DemoX+1T2015+type@asset+block@File-Name.pdf
        #  https://www.google.ro/maps/place/Romania/@45.1234567,25.0123456,4z/
    )
    def test_skip_non_emails(self, text):
        raw = self.SIMPLE_CONTEXT.format(text)
        result = obfuscate_util.find_emails(raw)
        self.assertEquals(raw, result)

    @data(
        (
            'https://example.com/stream?q=user:acct:person@example.com',
            'https://example.com/stream?q=user:acct:<<EMAIL>>'
        ),
        (' (mailto:course@organization.edu), ', ' (mailto:<<EMAIL>>), '),
        # At some point, it would be good to broaden the target for email
        # matching to include mail headers that include a full name.  I.e.:
        # 'From: "First Last" <firstlast@example.com.au>' =>
        #  'From: <<EMAIL>>' instead of 'From: "First Last" <<<EMAIL>>>'.
    )
    @unpack
    def test_find_emails_in_context(self, text, result):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format(result)
        actual = obfuscate_util.find_emails(raw)
        self.assertEquals(expected, actual)

    #####################
    # username
    #####################

    @data(
        ('username', 'username'),
        ('Username', 'username'),
        ('UserName', 'username'),
        ('USERNAME', 'username'),
        ('Username1234', 'username1234'),
        ('User-name', 'user-name'),
        # We do not expect there to be usernames with non-ascii characters.
        # ('Øyaland'.decode('utf8'), 'Øyaland'.decode('utf8')),
        # However, there are usernames that contain leading and/or trailing dashes, and this
        # confuses the regex as well.  That's because the \b boundaries don't match properly.
        # ('-User-name-', '-user-name-'),
    )
    @unpack
    def test_find_simple_usernames(self, text, username):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format("<<USERNAME>>")
        result = obfuscate_util.find_username(raw, username)
        self.assertEquals(expected, result)

    @data(
        ('ausername', 'username'),
    )
    @unpack
    def test_skip_non_usernames(self, text, username):
        raw = self.SIMPLE_CONTEXT.format(text)
        result = obfuscate_util.find_username(raw, username)
        self.assertEquals(raw, result)

    @data(
        ("My name is Username, I'm from A.", "username", "My name is <<USERNAME>>, I'm from A."),
        ("My name is Username12345. I'm from A.", "username12345", "My name is <<USERNAME>>. I'm from A."),
        ("My name is John (username), I'm from A.", "username", "My name is John (<<USERNAME>>), I'm from A."),
        (
            "My name is John (name=username), I'm from A.", "username",
            "My name is John (name=<<USERNAME>>), I'm from A."
        ),
        (
            "Visit my website http://username.com/this/link.", "username",
            "Visit my website http://<<USERNAME>>.com/this/link."
        ),
        ("http://instagram.com/username", "username", "http://instagram.com/<<USERNAME>>"),
        ("[http://twitter.com/username]", "username", "[http://twitter.com/<<USERNAME>>]"),
    )
    @unpack
    def test_find_usernames_in_context(self, text, username, result):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format(result)
        actual = obfuscate_util.find_username(raw, username)
        self.assertEquals(expected, actual)

    #####################
    # fullname
    #####################

    @data(
        ('first', 'First Last'),
        ('last', 'First Last'),
        ('first last', 'First Last'),
        ('first1234', 'First1234 Last'),
        ('FIRST', 'First Last'),
        ('LAST', 'First Last'),
        ('First', 'first last'),
        ('First-name', 'First-name Last-name'),
        ('First', '   first last   '),
        ('First', '  Last, First   '),
        ('Last', '  Last, First   '),
        ('Last', '**First Last**'),
        ('Last', '"First Last"'),
        ('Last', '"First" "Last"'),
        ('Last', 'First M. Last'),
        ('Last', 'First (Last)'),
        ("O'Last", "First O'Last"),
        ('MacLast', 'First MacLast'),
        ('Björk'.decode('utf8'), 'Björn Björk'.decode('utf8')),
        ('Olav Øyaland'.decode('utf8'), 'Olav Øyaland'.decode('utf8')),
        ('Øyaland'.decode('utf8'), 'Øyaland'.decode('utf8')),
        (u'T\u00e9st', u'my t\u00e9st'),
        # Nicknames and alternate forms of names are not found.
        # ('My name is Rob', 'Robert Last'),
    )
    @unpack
    def test_find_simple_fullnames(self, text, fullname):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format("<<FULLNAME>>")
        result = obfuscate_util.find_user_fullname(raw, fullname)
        self.assertEquals(expected, result)

    @data(
        ('the best is what I am', 'I am the Beast'),
        ('the best is what I am', 'I am The Beast'),
        # These are example overflags:
        # ('a mark on your paper', 'Mark Last'),
        # ('he said you would come', 'Said Last'),
        # ('he felt great joy', 'Joy Last'),
    )
    @unpack
    def test_skip_non_fullnames(self, text, fullname):
        raw = self.SIMPLE_CONTEXT.format(text)
        result = obfuscate_util.find_user_fullname(raw, fullname)
        self.assertEquals(raw, result)

    @data(
        ('My name is First Last.', 'First Last', 'My name is <<FULLNAME>>.'),
        ('My name is First,I am taking', 'First Last', 'My name is <<FULLNAME>>,I am taking'),
        ('My name is First Last.', 'First Last', 'My name is <<FULLNAME>>.'),
        ('www.linkedin.com/pub/first-last-last2/1/200/190/', 'First Last-Last2',
         'www.linkedin.com/pub/<<FULLNAME>>-<<FULLNAME>>/1/200/190/'),
        ('My name is First Maiden Last.', 'First Last', 'My name is <<FULLNAME>> Maiden <<FULLNAME>>.'),
        (u'This is a (T\u00e9st)', u'my t\u00e9st', 'This is a (<<FULLNAME>>)'),
    )
    @unpack
    def test_find_fullnames_in_context(self, text, fullname, result):
        raw = self.SIMPLE_CONTEXT.format(text)
        expected = self.SIMPLE_CONTEXT.format(result)
        actual = obfuscate_util.find_user_fullname(raw, fullname)
        self.assertEquals(expected, actual)

    @data(
        u'a/l',
        u'???',
        u'???',  # Test caching by calling twice
        u'First \u201cNickname\u201d Last',
        u'user@example.com',
        u'Jos\ufffd Last',
        u'FIRST LAST S/O FATHERFIRST FATHERLAST,DOB:01/01/1961',
    )
    def test_reject_bad_fullnames(self, fullname):
        raw = self.SIMPLE_CONTEXT.format('dummy')
        result = obfuscate_util.find_user_fullname(raw, fullname)
        self.assertEquals(raw, result)
        self.assertTrue(fullname in obfuscate_util.REJECTED_NAMES)


@ddt
class FindMatchLogContextTestCase(TestCase):
    """Test finding matches for regular expressions in strings."""

    def setUp(self):
        super(FindMatchLogContextTestCase, self).setUp()
        patcher = patch('edx.analytics.tasks.util.obfuscate_util.log')
        self.mock_log = patcher.start()
        self.addCleanup(patcher.stop)

    LEFT_CONTEXT = u" This is left context: "
    RIGHT_CONTEXT = u" This is right context. "
    TEXT = '240-123-4567'
    TYPE = 'PHONE_NUMBER'

    def test_no_logging(self):
        raw = "{}{}{}".format(self.LEFT_CONTEXT, self.TEXT, self.RIGHT_CONTEXT)
        obfuscate_util.find_phone_numbers(raw)
        self.assertEquals(self.mock_log.info.call_count, 0)

    def find_logged_contexts(self, raw, log_context):
        """Pull out strings from context-logging, to allow that to be tested."""
        obfuscate_util.find_phone_numbers(raw, log_context=log_context)
        self.assertEquals(self.mock_log.info.call_count, 1)
        args, _ = self.mock_log.info.call_args
        self.assertEquals(len(args), 5)
        self.assertEquals(args[1], self.TYPE)
        self.assertEquals(args[3], self.TEXT)
        return (args[2], args[4])

    def test_no_context(self):
        raw = "{}{}{}".format(self.LEFT_CONTEXT, self.TEXT, self.RIGHT_CONTEXT)
        left, right = self.find_logged_contexts(raw, log_context=0)
        self.assertEquals(left, '')
        self.assertEquals(right, '')

    def test_all_context(self):
        raw = "{}{}{}".format(self.LEFT_CONTEXT, self.TEXT, self.RIGHT_CONTEXT)
        left, right = self.find_logged_contexts(raw, log_context=50)
        self.assertEquals(left, self.LEFT_CONTEXT)
        self.assertEquals(right, self.RIGHT_CONTEXT)

    def test_some_context(self):
        raw = "{}{}{}".format(self.LEFT_CONTEXT, self.TEXT, self.RIGHT_CONTEXT)
        left, right = self.find_logged_contexts(raw, log_context=10)
        self.assertEquals(left, self.LEFT_CONTEXT[-10:])
        self.assertEquals(right, self.RIGHT_CONTEXT[:10])

    def test_no_left_context(self):
        raw = "{}{}".format(self.TEXT, self.RIGHT_CONTEXT)
        left, right = self.find_logged_contexts(raw, log_context=10)
        self.assertEquals(left, "")
        self.assertEquals(right, self.RIGHT_CONTEXT[:10])

    def test_no_right_context(self):
        raw = "{}{}".format(self.LEFT_CONTEXT, self.TEXT)
        left, right = self.find_logged_contexts(raw, log_context=10)
        self.assertEquals(left, self.LEFT_CONTEXT[-10:])
        self.assertEquals(right, "")

    def test_multiple_matches(self):
        raw = "{}{}{}{}{}{}{}".format(
            self.LEFT_CONTEXT, self.TEXT, self.RIGHT_CONTEXT, self.TEXT,
            self.RIGHT_CONTEXT, self.TEXT, self.LEFT_CONTEXT
        )
        obfuscate_util.find_phone_numbers(raw, log_context=10)
        self.assertEquals(self.mock_log.info.call_count, 3)
        args_list = self.mock_log.info.call_args_list
        # Create context lists in reverse so they can be popped.
        left_contexts = [self.RIGHT_CONTEXT, self.RIGHT_CONTEXT, self.LEFT_CONTEXT]
        right_contexts = list(reversed(left_contexts))
        for args, _ in args_list:
            self.assertEquals(len(args), 5)
            self.assertEquals(args[1], self.TYPE)
            self.assertEquals(args[3], self.TEXT)
            left = left_contexts.pop()
            right = right_contexts.pop()
            self.assertEquals(args[2], left[-10:])
            self.assertEquals(args[4], right[:10])


# Create default tables that use arbitrary leading whitespace before the first field value,
# but tabs as the between-field delimiter.  That way, fields can contain spaces (as in full names).
DEFAULT_AUTH_USER = """
    1	honor
    2	audit
    3	verified
    4 	staff
"""

DEFAULT_AUTH_USER_PROFILE = """
    1	Honor Student
    2	Audit John
    3	Verified Vera
    4	Static Staff
"""


def get_mock_user_info_requirements(auth_user=DEFAULT_AUTH_USER, auth_user_profile=DEFAULT_AUTH_USER_PROFILE):
    """Replacement for get_user_info_requirements for testing purposes."""

    def reformat_as_sqoop_output(string):
        """Convert tab-delimited data to look like Sqoop output."""
        return textwrap.dedent(string).strip().replace('\t', '\x01')

    # These keys need to return a fake Task whose output() is a fake Target.
    user_info_setup = {
        'auth_user': FakeTask(value=reformat_as_sqoop_output(auth_user)),
        'auth_userprofile': FakeTask(value=reformat_as_sqoop_output(auth_user_profile)),
    }
    return MagicMock(return_value=user_info_setup)


class UserInfoTestCase(TestCase):
    """Test encoding and decoding of backslashed data."""

    def setUp(self):
        super(UserInfoTestCase, self).setUp()
        obfuscate_util.reset_user_info_for_testing()

    def test_default(self):
        user_info = obfuscate_util.UserInfoMixin()
        user_info.user_info_requirements = get_mock_user_info_requirements()
        self.assertDictEqual(user_info.user_by_id, {
            1: {'user_id': 1, 'username': 'honor', 'name': 'Honor Student'},
            2: {'user_id': 2, 'username': 'audit', 'name': 'Audit John'},
            3: {'user_id': 3, 'username': 'verified', 'name': 'Verified Vera'},
            4: {'user_id': 4, 'username': 'staff', 'name': 'Static Staff'},
        })
        self.assertDictEqual(user_info.user_by_username, {
            'honor': {'user_id': 1, 'username': 'honor', 'name': 'Honor Student'},
            'audit': {'user_id': 2, 'username': 'audit', 'name': 'Audit John'},
            'verified': {'user_id': 3, 'username': 'verified', 'name': 'Verified Vera'},
            'staff': {'user_id': 4, 'username': 'staff', 'name': 'Static Staff'},
        })

    def test_with_metadata(self):
        metadata = '{"start_time": "2015-01-01T10:30:35.123456", "end_time": "2016-01-01T-09:15:13.654321"}'
        my_auth_user = '{}1\ttest'.format(metadata)
        my_auth_user_profile = '{}1\tTest User'.format(metadata)
        user_info = obfuscate_util.UserInfoMixin()
        user_info.user_info_requirements = get_mock_user_info_requirements(
            auth_user=my_auth_user, auth_user_profile=my_auth_user_profile
        )
        self.assertDictEqual(user_info.user_by_id, {
            1: {'user_id': 1, 'username': 'test', 'name': 'Test User'},
        })
        self.assertDictEqual(user_info.user_by_username, {
            'test': {'user_id': 1, 'username': 'test', 'name': 'Test User'},
        })

    def test_with_older_auth_user(self):
        my_auth_user = '1\ttest'
        user_info = obfuscate_util.UserInfoMixin()
        user_info.user_info_requirements = get_mock_user_info_requirements(
            auth_user=my_auth_user,
        )
        self.assertDictEqual(user_info.user_by_id, {
            1: {'user_id': 1, 'username': 'test', 'name': 'Honor Student'},
        })
        self.assertDictEqual(user_info.user_by_username, {
            'test': {'user_id': 1, 'username': 'test', 'name': 'Honor Student'},
        })

    def test_with_older_auth_user_profile(self):
        my_auth_user_profile = '1\tTest User'
        user_info = obfuscate_util.UserInfoMixin()
        user_info.user_info_requirements = get_mock_user_info_requirements(
            auth_user_profile=my_auth_user_profile,
        )
        self.assertDictEqual(user_info.user_by_id, {
            1: {'user_id': 1, 'username': 'honor', 'name': 'Test User'},
            2: {'user_id': 2, 'username': 'audit'},
            3: {'user_id': 3, 'username': 'verified'},
            4: {'user_id': 4, 'username': 'staff'},
        })
        self.assertDictEqual(user_info.user_by_username, {
            'honor': {'user_id': 1, 'username': 'honor', 'name': 'Test User'},
            'audit': {'user_id': 2, 'username': 'audit'},
            'verified': {'user_id': 3, 'username': 'verified'},
            'staff': {'user_id': 4, 'username': 'staff'},
        })

    def test_with_bad_profile_entry(self):
        my_auth_user = """
             1	test
             2	test2
        """
        my_auth_user_profile = """
             1	Test User
             two	Second User
        """
        user_info = obfuscate_util.UserInfoMixin()
        user_info.user_info_requirements = get_mock_user_info_requirements(
            auth_user=my_auth_user, auth_user_profile=my_auth_user_profile
        )
        self.assertDictEqual(user_info.user_by_id, {
            1: {'user_id': 1, 'username': 'test', 'name': 'Test User'},
            2: {'user_id': 2, 'username': 'test2'},
        })
        self.assertDictEqual(user_info.user_by_username, {
            'test': {'user_id': 1, 'username': 'test', 'name': 'Test User'},
            'test2': {'user_id': 2, 'username': 'test2'},
        })

    def test_with_bad_user_entry(self):
        my_auth_user = """
             1	test
             two	test2
        """
        my_auth_user_profile = """
             1	Test User
             2	Second User
        """
        user_info = obfuscate_util.UserInfoMixin()
        user_info.user_info_requirements = get_mock_user_info_requirements(
            auth_user=my_auth_user, auth_user_profile=my_auth_user_profile
        )
        self.assertDictEqual(user_info.user_by_id, {
            1: {'user_id': 1, 'username': 'test', 'name': 'Test User'},
        })
        self.assertDictEqual(user_info.user_by_username, {
            'test': {'user_id': 1, 'username': 'test', 'name': 'Test User'},
        })

    def test_with_empty_username(self):
        my_auth_user = """
             1	test
             2	{intentional_blank}
             3	test3
        """.format(intentional_blank='   ')
        my_auth_user_profile = """
             1	Test User
             2	Second User
             3	Third User
        """
        user_info = obfuscate_util.UserInfoMixin()
        user_info.user_info_requirements = get_mock_user_info_requirements(
            auth_user=my_auth_user, auth_user_profile=my_auth_user_profile
        )
        self.assertDictEqual(user_info.user_by_id, {
            1: {'user_id': 1, 'username': 'test', 'name': 'Test User'},
            3: {'user_id': 3, 'username': 'test3', 'name': 'Third User'},
        })
        self.assertDictEqual(user_info.user_by_username, {
            'test': {'user_id': 1, 'username': 'test', 'name': 'Test User'},
            'test3': {'user_id': 3, 'username': 'test3', 'name': 'Third User'},
        })


@ddt
class ObfuscatorTestCase(TestCase):
    """Test Obfuscator methods."""

    def test_obfuscate_email_before_username(self):
        obfuscator = obfuscate_util.Obfuscator()
        input_text = "Email is testusername@example.com."
        result = obfuscator.obfuscate_text(
            input_text,
            user_info={'username': ['testusername']},
        )
        self.assertEquals(result, 'Email is <<EMAIL>>.')
        result = obfuscator.obfuscate_text(
            input_text,
            user_info={'username': ['testusername']},
            entities=['username'],
        )
        self.assertEquals(result, 'Email is <<USERNAME>>@example.com.')

    @data(
        ('testusername@email.com', ['email']),
        ('testusername', ['username']),
        ('Test User', ['fullname']),
        ('213-456-7890', ['phone']),
        ('12345', ['userid']),
        ('240-123-4567', ['possible_phone']),
        ('phone number', ['phone_context']),
        ('my name', ['name_context']),
        ('write me', ['email_context']),
        ('https://www.facebook.com/someusername', ['facebook']),
    )
    @unpack
    def test_entity_flags(self, text, entities):
        obfuscator = obfuscate_util.Obfuscator()
        result = obfuscator.obfuscate_text(
            text,
            user_info={
                'username': ['testusername'],
                'user_id': [12345],
                'name': ['Test User'],
            },
            entities=entities,
        )
        self.assertTrue(result.startswith('<<'))
        self.assertTrue(result.endswith('>>'))

    def test_obfuscate_list(self):
        input_obj = {
            'key': [
                {'email': 'test@example.com'},
                {'dummy': 'unchanging value'},
                {'phone': '213-456-7890'},
                {'list': ['unchanging list value']},
            ]
        }
        obfuscator = obfuscate_util.Obfuscator()
        result = obfuscator.obfuscate_structure(input_obj, 'root')
        expected = {'key': [
            {'email': '<<EMAIL>>'},
            {'dummy': 'unchanging value'},
            {'phone': '<<PHONE_NUMBER>>'},
            {'list': ['unchanging list value']},
        ]}
        self.assertEquals(result, expected)

    @data(
        ('n321-4567\\\\n', None),
        ('\n321-4567\\\\n', '\n<<PHONE_NUMBER>>\\\\n'),
        ('\\n321-4567\\\\n', '\\n<<PHONE_NUMBER>>\\\\n'),
        ('\\\n321-4567\\\\n', '\\\n<<PHONE_NUMBER>>\\\\n'),
        ('\\\\n321-4567\\\\n', '\\\\n<<PHONE_NUMBER>>\\\\n'),
    )
    @unpack
    def test_backslash_decoding(self, text, expected):
        obfuscator = obfuscate_util.Obfuscator()
        result = obfuscator.obfuscate_structure(text, 'root')
        self.assertEquals(result, expected)

    @data(
        (u'321-4567'.encode('utf8'), u'<<PHONE_NUMBER>>'.encode('utf8')),
        ('Test User', u'<<FULLNAME>>'.encode('utf8')),
        (u'Olav Øyaland'.encode('utf8'), u'<<FULLNAME>>'.encode('utf8')),
    )
    @unpack
    def test_unicode_decoding(self, text, expected):
        obfuscator = obfuscate_util.Obfuscator()
        user_info = {'name': ['Olav Øyaland'.decode('utf8'), 'Test User']}
        result = obfuscator.obfuscate_structure(text, 'root', user_info=user_info)
        self.assertEquals(result, expected)
