"""Utilities that are used for performing deidentification, or what passes for such."""

import re
import logging

import luigi

from edx.analytics.tasks.url import ExternalURL
from edx.analytics.tasks.util.id_codec import UserIdRemapperMixin


log = logging.getLogger(__name__)


# Define user-info maps to be global scope.
_user_by_id = None
_user_by_username = None


class UserInfoDownstreamMixin(object):

    auth_user_path = luigi.Parameter()
    auth_userprofile_path = luigi.Parameter()


class UserInfoMixin(UserInfoDownstreamMixin):

    def user_info_requirements(self):
        return {
            # 'auth_user': ExternalURL(self.auth_user_path.rstrip('/') + '/'),
            # 'auth_userprofile': ExternalURL(self.auth_userprofile_path.rstrip('/') + '/')
            'auth_user': ExternalURL(self.auth_user_path),
            'auth_userprofile': ExternalURL(self.auth_userprofile_path),
        }

    @property
    def user_by_id(self):
        """
        For id as key, returns dict with 'username', 'user_id', and 'name' as keys.

        id should be an int.

        'user_id' value returned is an iterable of ints.
        'name' and 'username' values returned are iterables of unicode strings.
        'name' may not always be present.
        """
        self._initialize_user_info()
        return _user_by_id

    @property
    def user_by_username(self):
        """
        For username as key, returns dict with 'username', 'user_id', and 'name' as keys.

        Username should be a unicode string.

        'user_id' value returned is an iterable of ints.
        'name' and 'username' values returned are iterables of unicode strings.
        'name' may not always be present.
        """
        self._initialize_user_info()
        return _user_by_username

    def _initialize_user_info(self):
        # TODO:  docstring.
        global _user_by_id
        global _user_by_username

        if _user_by_id is None:
            log.info("Loading user_info data.")
            try:
                _user_by_id = {}
                _user_by_username = {}
                count = 0
                input_targets = {k: v.output() for k, v in self.user_info_requirements().items()}
                with input_targets['auth_user'].open('r') as auth_user_file:
                    for line in auth_user_file:
                        count += 1
                        # TODO: Fix ugly hack to get around reading .metadata record information.
                        if line.startswith('{'):
                            line = line.split('}', 2)[1]
                        split_line = line.rstrip('\r\n').split('\x01')
                        try:
                            user_id = int(split_line[0])
                        except ValueError:
                            log.error("Unexpected non-int value for user_id read from auth_user file: %s", split_line)
                            continue
                        username = split_line[1].decode('utf8').strip()
                        if len(username) == 0:
                            log.error("Unexpected whitespace value for username read from auth_user file: %s", split_line)
                            continue
                        _user_by_id[user_id] = {'username': username, 'user_id': user_id}
                        # Point to the same object so that we can just store two pointers to the data instead of two
                        # copies of the data
                        _user_by_username[username] = _user_by_id[user_id]
                    log.info("Finished loading %s auth_user records from %s into user_info data.", count, input_targets['auth_user'].path)

                count = 0
                with input_targets['auth_userprofile'].open('r') as auth_user_profile_file:
                    for line in auth_user_profile_file:
                        count += 1
                        # TODO: Fix ugly hack to get around reading .metadata record information.
                        if line.startswith('{'):
                            line = line.split('}', 2)[1]
                        split_line = line.rstrip('\r\n').split('\x01')
                        try:
                            user_id = int(split_line[0])
                        except ValueError:
                            log.error("Unexpected non-int value for user_id read from auth_user_profile file: %s", split_line)
                            continue
                        name = split_line[1].decode('utf8')
                        try:
                            _user_by_id[user_id]['name'] = name
                        except KeyError:
                            # TODO: Look at whether this will break if the userprofile is more recent than the
                            # auth_user file.  (We have no guarantee that they are dumped at the same time,
                            # though we presume they were dumped on the same day, and presumably closer in time than that.)
                            # So is this what we want the behavior to be?  Or rather to just not have a username for
                            # this id?
                            log.error("Unknown value for user_id read from auth_user_profile file: %s '%s'", user_id, name)
                            pass
                    log.info("Finished loading %s auth_userprofile records from %s into user_info data.", count, input_targets['auth_userprofile'].path)

            except Exception:
                # Don't leave a half-initialized set of structures for the next task to use.
                log.info("Failed to load user_info data -- resetting.")
                _user_by_id = None
                _user_by_username = None
                raise

            log.info("Loaded user_info data.")


class DeidentifierDownstreamMixin(UserInfoDownstreamMixin):

    entities = luigi.Parameter(is_list=True, default=[])
    log_context = luigi.IntParameter(default=None)


class DeidentifierMixin(UserIdRemapperMixin, DeidentifierDownstreamMixin, UserInfoMixin):

    def __init__(self, *args, **kwargs):
        super(DeidentifierMixin, self).__init__(*args, **kwargs)
        deid_args = {}
        if 'entities' in kwargs and len(kwargs['entities']) > 0:
            deid_args['entities'] = set(kwargs['entities'])
        if 'log_context' in kwargs:
            deid_args['log_context'] = kwargs['log_context']
        self.deidentifier = Deidentifier(**deid_args)


def backslash_decode_value(value):
    return value.replace('\\\\', '<<BACKSLASH>>').replace('\\r', '\r').replace('\\t', '\t').replace('\\n', '\n').replace('<<BACKSLASH>>', '\\')


def backslash_encode_value(value):
    return value.replace('\\', '\\\\').replace('\r', '\\r').replace('\t', '\\t').replace('\n', '\\n')


# Find \\r, \\t or \\n
BACKSLASH_PATTERN = re.compile(r'\\[rtn]')


def needs_backslash_decoding(value):
    # If there are any decoded values already present, then don't decode further.
    if '\t' in value or '\n' in value or '\r' in value:
        return False

    # Check to see if there is anything that should be decoded.
    match = re.search(BACKSLASH_PATTERN, value)
    return True if match else False


# A negative value for log_context disables logging of matches.
DEFAULT_LOG_CONTEXT = -1


def find_all_matches(pattern, string, label, log_context=DEFAULT_LOG_CONTEXT):
    """
    Applies pattern to string and replaces all matches with <<label>>.

    A positive value for log_context provides up to that number of characters on left and right
    of match, if present.

    A negative value for log_context disables logging of matches.
    """
    output = []
    output_end = 0
    matches = pattern.finditer(string)
    for match in matches:
        start, end = match.span()
        if log_context >= 0:
            left_edge = start - log_context if start > log_context else 0
            left = backslash_encode_value(string[left_edge:start])
            right = backslash_encode_value(string[end:end + log_context])
            value1 = match.group(0)
            value = unicode(value1)
            log.info(u"Found %s:  %s<<%s>>%s", label, left, value, right)
        output.append(string[output_end:start])
        output.append("<<{}>>".format(label))
        output_end = end
    if output_end > 0:
        output.append(string[output_end:])
        return "".join(output)
    else:
        return string


#####################
# Phone numbers
#####################

# We're not trying to validate input, but rather suppress output, so we can use a simpler pattern.

# First digit cannot be '1'.  Leave off extensions for now.  Requires
# a delimiter.  Permit a leading 1 or +1.  Won't match (123)456-7890
# because of the leading 1.  No one would write (123).456 or
# (123)-456, so make that optional.
US_PHONE_PATTERN = r"""(?:\+?1\s*(?:[.\- ]?\s*)?)?  # possible leading "+1"
                       (?:\([2-9]\d{2}\)\s*|[2-9]\d{2}\s*(?:[.\- ]\s*))?  # 3-digit area code, in parens or not
                       \b\d{3}\s*(?:[\- ]\s*)\d{4}"""   # regular 7-digit phone.  Note no use of dot here: too much like a float.


# INTL_PHONE_PATTERN = r'\b(\+(9[976]\d|8[987530]\d|6[987]\d|5[90]\d|42\d|3[875]\d|2[98654321]\d|9[8543210]|8[6421]|6[6543210]|5[87654321]|4[987654310]|3[9643210]|2[70]|7|1)\d{1,14}$
# Starts with +, then some digits which may have spaces, ending with a digit.
INTL_PHONE_PATTERN = r'\+(?:\d[\- ]?){6,14}\d'

# Note that we don't use the \b at the beginning: it's not a \W to \w transition when leading with paren or plus.
PHONE_PATTERN = r'((?:' + US_PHONE_PATTERN + r'|' + INTL_PHONE_PATTERN + r'))\b'

# http://blog.stevenlevithan.com/archives/validate-phone-number
POSSIBLE_PHONE_PATTERN = r'(\+?\b[\d][0-9 \(\).\-]{8,}[\d])\b'


COMPILED_PHONE_PATTERN = re.compile(PHONE_PATTERN, re.VERBOSE)
COMPILED_POSSIBLE_PHONE_PATTERN = re.compile(POSSIBLE_PHONE_PATTERN, re.VERBOSE)


def find_phone_numbers(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(COMPILED_PHONE_PATTERN, text, "PHONE_NUMBER", log_context)


def find_possible_phone_numbers(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(COMPILED_POSSIBLE_PHONE_PATTERN, text, "POSSIBLE_PHONE_NUMBER", log_context)


#####################
# Personal context
#####################

EMAIL_CONTEXT = re.compile(
    r'\b(my (?:personal )?e[\- ]?mail|e[\- ]mail me|e[\- ]mail(?: address)?|send e[\- ]mail|write me|talk with me|Skype|address|facebook)\b',
    re.IGNORECASE,
)


def find_email_context(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(EMAIL_CONTEXT, text, "EMAIL_CONTEXT", log_context)

NAME_CONTEXT = re.compile(
    r'\b(hi|hello|sincerely|yours truly|Dear|Mr|Ms|Mrs|regards|cordially|best wishes|cheers|my name)\b',
    re.IGNORECASE,
)


def find_name_context(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(NAME_CONTEXT, text, "NAME_CONTEXT", log_context)

PHONE_CONTEXT = re.compile(
    r'(\bphone:|\bp:|b\c:|\bcall me\b|\(home\)|\(cell\)|my phone|phone number)',
    re.IGNORECASE,
)


def find_phone_context(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(PHONE_CONTEXT, text, "PHONE_CONTEXT", log_context)


#####################
# Facebook
#####################

# https://www.facebook.com/user.name
FACEBOOK_PATTERN = re.compile(
    r'\b(https:\/\/www\.facebook\.com\/[\w.]+)\b',
    re.IGNORECASE,
)


def find_facebook(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(FACEBOOK_PATTERN, text, "FACEBOOK", log_context)


#####################
# Zip codes
#####################

# Look for a leading space.
ZIPCODE_PATTERN = r'((?<= )\b\d{5}(?:[-\s]\d{4})?\b)'
COMPILED_ZIPCODE_PATTERN = re.compile(ZIPCODE_PATTERN, re.IGNORECASE)


def find_zipcodes(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(COMPILED_ZIPCODE_PATTERN, text, "ZIPCODE", log_context)


#####################
# EMAIL
#####################

EMAIL_PATTERN = r'((?<=\s)([a-zA-Z0-9\(\.\-]+)[@]([a-zA-Z0-9\.]+)\.(?:edu|com|org)\b)'
ORIG_EMAIL_PATTERN = r'(.*)\s+(([a-zA-Z0-9\(\.\-]+)[@]([a-zA-Z0-9\.]+)(.)(edu|com))\\s*(.*)'
#emailPattern='(.*)\\s+([a-zA-Z0-9\\.]+)\\s*(\\(f.*b.*)?(@)\\s*([a-zA-Z0-9\\.\\s;]+)\\s*(\\.)\\s*(edu|com)\\s+(.*)'
COMPILED_EMAIL_PATTERN = re.compile(EMAIL_PATTERN, re.IGNORECASE)

# http://www.regular-expressions.info/email.html
BASIC_EMAIL_PATTERN = r'\b[a-z0-9!#$%&\'*+\/\=\?\^\_\`\{\|\}\~\-]+(?:\.[a-z0-9!#$%&\'*+\/\=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\b'
# case-insensitive didn't work properly for cap-init cases, so add A-Z explicitly:
# BASIC_EMAIL_PATTERN = r'\b[A-Za-z0-9!#$%&\'*+\/\=\?\^\_\`\{\|\}\~\-]+(?:\.[A-Za-z0-9!#$%&\'*+\/\=?^_`{|}~-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\b'

COMPILED_POSSIBLE_EMAIL_PATTERN = re.compile(BASIC_EMAIL_PATTERN, re.IGNORECASE)


def find_emails(text, log_context=DEFAULT_LOG_CONTEXT):
    return find_all_matches(COMPILED_EMAIL_PATTERN, text, "ORIG_EMAIL", log_context)


def find_possible_emails(text, log_context=DEFAULT_LOG_CONTEXT):
    if '@' in text:
        return find_all_matches(COMPILED_POSSIBLE_EMAIL_PATTERN, text, "EMAIL", log_context)
    else:
        return text

# Some failures:
#
# Regular is too limited, so it truncates:
# Found EMAIL:  ed to my account in <<blah@yahoomail.com>>.au

# But extended doesn't deal well with case!
# Found POSSIBLE_EMAIL:  il me at First.L<<ast@example.co.uk>>.

# Leaving off name that precedes, as in email headers:
# Found POSSIBLE_EMAIL:  rom: "First Last" <<<firstlast@example.com.au>>>

#####################
# username
#####################


def find_username(text, username, log_context=DEFAULT_LOG_CONTEXT):
    username_pattern = re.compile(
        r'\b({})\b'.format(username),
        re.IGNORECASE,
    )
    return find_all_matches(username_pattern, text, "USERNAME", log_context)


#####################
# userid
#####################


def find_userid(text, user_id, log_context=DEFAULT_LOG_CONTEXT):
    userid_pattern = re.compile(
        r'\b({})\b'.format(user_id),
        re.IGNORECASE,
    )
    return find_all_matches(userid_pattern, text, "USER_ID", log_context)


#####################
# user profile => fullname
#####################

# Note the addition of the '$' at the
# end: match() only constrains the beginning.
LEGAL_NAME_PATTERN = re.compile(r"[\w. \-\'\(\)\,\*\"]+$", re.UNICODE)

# Cache a set of names that have been rejected, so that we don't retry
# them repeatedly.  Also cuts down on logging noise.
REJECTED_NAMES = set()

# People use common words in their names.  Sometimes it's just plain text; sometimes jibberish.
# It's hard to tell the difference.  (E.g. "The" appears in Vietnamese-like names.)
STOPWORDS = ['the', 'and', 'can']


def find_user_fullname(text, fullname, log_context=DEFAULT_LOG_CONTEXT):

    if fullname in REJECTED_NAMES:
        return text

    # Indian names use special abbreviations for "son of"/"daughter of".
    # For the purposes of finding matches, just strip these out.
    # s/o = son of, d/o = daughter of, Others are: a/l = son of (Malay),
    # a/p = daughter of (Malay).  Also a/k, w/o.
    fullname2 = re.sub(r'\b(?:s\/o|d\/o|a\/l|a\/p|a\/k|w\/o)\b', ' ', fullname, flags=re.IGNORECASE)

    # Check the name for bogus characters.  They may have special meanings if
    # embedded in a regexp.  (We should probably be escaping any periods,
    # if we were even including them.)  In fact, we should be using re.escape.
    # TODO: switch to use re.escape.
    if not LEGAL_NAME_PATTERN.match(fullname2):
        log.error(u"Fullname '%r' contains unexpected characters.", fullname)
        REJECTED_NAMES.add(fullname)
        return text

    # Strip parentheses and comma and the like, and escape the characters that are
    # legal in names but may have different meanings in regexps (i.e. apostrophe and period).
    # TODO: if a single comma is found, maybe linearize the name by swapping the delimited parts?
    fullname2 = fullname2.replace('"', ' ').replace('*', ' ').replace('(', ' ').replace(')', ' ').replace(',', ' ').replace('-', '\\-').replace("'", "\\'").replace('.', '\\.')

    # Create regexp by breaking the name up.  Assume spaces.
    names = fullname2.strip().split()
    if len(names) == 0:
        log.error(u"Fullname '%r' contains only whitespace characters.", fullname)
        REJECTED_NAMES.add(fullname)
        return text

    patterns = []
    # add the whole, then add each individual part if it's long enough.
    patterns.append(u" ".join(names))
    for name in names:
        if len(name) > 2 and name not in STOPWORDS and not name.endswith('.'):
            patterns.append(name)

    # Because we're operating with unicode instead of raw strings, make sure that
    # the slashes are escaped.
    fullname_pattern = re.compile(
        u'\\b({})\\b'.format(u"|".join(patterns)),
        re.IGNORECASE,
    )
    return find_all_matches(fullname_pattern, text, "FULLNAME", log_context)


#################
# General deidentification
#################

DEFAULT_ENTITIES = set(['email', 'username', 'fullname', 'phone', 'userid'])


class Deidentifier(object):

    log_context = DEFAULT_LOG_CONTEXT
    entities = DEFAULT_ENTITIES

    def __init__(self, **kwargs):
        if 'log_context' in kwargs:
            self.log_context = kwargs['log_context']
        if 'entities' in kwargs:
            self.entities = kwargs['entities']

    def is_logging_enabled(self):
        return (self.log_context > 0)

    def deidentify_text(self, text, user_info=None, log_context=None, entities=None):
        """
        Applies all selected deidentification patterns to text.

        user_info is a dict (or namedtuple.__dict__), with 'username', 'user_id' and 'name' keys, if known.
            Values should be lists containing the value or values of that kind of data.  (That way,
            we can look for more than one username in a forum post, for example.)

        log_context specifies the amount of context on either side of matches, when logging.

        entities is a set with elements to indicate if a search for a given entity should be performed.
            Main production choices include:

            'email'
            'username'
            'fullname'
            'phone'
            'userid'

            Additions for development include:

            'facebook'
            'possible_phone'
            'email_context'
            'phone_context'
            'name_context'

        """
        if log_context is None:
            log_context = self.log_context
        if entities is None:
            entities = self.entities

        # Names can appear in emails and identifying urls, so find them before the names.
        if 'email' in entities:
            text = find_emails(text, log_context)
            text = find_possible_emails(text, log_context)
        if 'facebook' in entities:
            text = find_facebook(text, log_context)

        # Find Names and IDs, using supplied information to search for.
        if user_info is not None:
            if 'fullname' in entities:
                for fullname in user_info.get('name', []):
                    text = find_user_fullname(text, fullname, log_context)

            if 'username' in entities:
                for username in user_info.get('username', []):
                    text = find_username(text, username, log_context)

            if 'userid' in entities:
                for user_id in user_info.get('user_id', []):
                    text = find_userid(text, user_id, log_context)

        # Find phone numbers.
        if 'phone' in entities:
            text = find_phone_numbers(text, log_context)
        if 'possible_phone' in entities:
            text = find_possible_phone_numbers(text, log_context)

        # Look for context *after* looking for items?
        # (If we need the original item for context, then we should do
        # context first, but it must not overlap with actual item.)
        # E.g. "facebook" in context and in url.
        if 'email_context' in entities:
            text = find_email_context(text, log_context)
        if 'phone_context' in entities:
            text = find_phone_context(text, log_context)
        if 'name_context' in entities:
            text = find_name_context(text, log_context)

        # text = find_zipcodes(text, log_context)
        return text

    def deidentify_structure(self, obj, label, user_info=None, log_context=None, entities=None):
        """Returns a modified object if any string contained within it was deidentified, None otherwise."""

        if isinstance(obj, dict):
            new_dict = {}
            changed = False
            for key in obj.keys():
                value = obj.get(key)
                if isinstance(key, str):
                    new_label = u"{}.{}".format(label, key.decode('utf8'))
                else:
                    new_label = u"{}.{}".format(label, key)
                updated_value = self.deidentify_structure(value, new_label, user_info, log_context, entities)
                if updated_value is not None:
                    changed = True
                    new_dict[key] = updated_value
                else:
                    new_dict[key] = value
            if changed:
                return new_dict
            else:
                return None
        elif isinstance(obj, list):
            new_list = []
            changed = False
            for index, value in enumerate(obj):
                new_label = u"{}[{}]".format(label, index)
                updated_value = self.deidentify_structure(value, new_label, user_info, log_context, entities)
                if updated_value is not None:
                    changed = True
                    new_list.append(updated_value)
                else:
                    new_list.append(value)
            if changed:
                return new_list
            else:
                return None
        elif isinstance(obj, unicode):
            # First perform backslash decoding on string, if needed.
            if needs_backslash_decoding(obj):
                decoded_obj = backslash_decode_value(obj)
                new_label = u"{}*d".format(label)
                updated_value = self.deidentify_structure(decoded_obj, new_label, user_info, log_context, entities)
                if updated_value is not None:
                    return backslash_encode_value(updated_value)
                else:
                    return None

            # Only deidentify once backslashes have been decoded as many times as needed.
            updated_value = self.deidentify_text(obj, user_info, log_context, entities)
            if obj != updated_value:
                if self.is_logging_enabled():
                    log.info(u"Deidentified '%s'", label)
                return updated_value
            else:
                return None
        elif isinstance(obj, str):
            unicode_obj = obj.decode('utf8')
            new_label = u"{}*u".format(label)
            updated_value = self.deidentify_structure(unicode_obj, new_label, user_info, log_context, entities)
            if updated_value is not None:
                return updated_value.encode('utf8')
            else:
                return None
        else:
            # It's an object, but not a string, list or dict.  Don't change it.  (It's probably an int.)
            return None


#################
# Implicit events
#################

# These patterns define the subset of events with implicit event_type
# values that are to be included in deidentified packages for RDX.
# (Note that a pattern not ending with '$' only needs to match the beginning
# of the event_type.)
IMPLICIT_EVENT_TYPE_PATTERNS = [
    r"^/courses/\(course_id\)/jump_to_id/",
    r"^/courses/\(course_id\)/courseware/",
    r"^/courses/\(course_id\)/info/?$",
    r"^/courses/\(course_id\)/progress/?$",
    r"^/courses/\(course_id\)/course_wiki/?$",
    r"^/courses/\(course_id\)/about/?$",
    r"^/courses/\(course_id\)/teams/?$",
    r"^/courses/\(course_id\)/[a-fA-F\d]{32}/?$",
    r"^/courses/\(course_id\)/?$",
    r"^/courses/\(course_id\)/pdfbook/\d+(/chapter/\d+(/\d+)?)?/?$",
    r"^/courses/\(course_id\)/wiki((?!/_).)*$",
    r"^/courses/\(course_id\)/discussion/(threads|comments)",
    r"^/courses/\(course_id\)/discussion/(upload|users|forum/?)$",
    r"^/courses/\(course_id\)/discussion/[\w\-.]+/threads/create$",
    r"^/courses/\(course_id\)/discussion/forum/[\w\-.]+/(inline|search|threads)$",
    r"^/courses/\(course_id\)/discussion/forum/[\w\-.]+/threads/\w+$",
]
