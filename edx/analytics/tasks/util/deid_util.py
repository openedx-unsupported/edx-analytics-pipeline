"""Utilities that are used for performing deidentification, or what passes for such."""

import re


def decode_value(value):
    return value.replace('\\\\', '<<BACKSLASH>>').replace('\\r', '\r').replace('\\t', '\t').replace('\\n', '\n').replace('<<BACKSLASH>>', '\\')


def encode_value(value):
    return unicode(value).replace('\\', '\\\\').replace('\r', '\\r').replace('\t','\\t').replace('\n', '\\n')


def find_all_matches(pattern, string, label, context=20):
    output = []
    output_end = 0
    # Note: adding re.IGNORECASE flag here *in addition* to in the
    # pattern results in no matches beginning in the first two
    # characters!  Weird!  So leave it out here!
    matches = pattern.finditer(string)
    for match in matches:
        start, end = match.span()
        left_edge = start - context if start > context else 0
        left = encode_value(string[left_edge:start])
        right = encode_value(string[end:end+context])
        value1 = match.group(0)
        value = unicode(value1)
        print u"Found {}:  {}<<{}>>{}".format(label, left, value, right).encode('utf-8')
        output.append(string[output_end:start])
        output.append("<<{}>>".format(label))
        output_end = end
    output.append(string[output_end:])
    return "".join(output)


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
                       \b\d{3}\s*(?:[.\- ]\s*)\d{4}"""   # regular 7-digit phone


# INTL_PHONE_PATTERN = r'\b(\+(9[976]\d|8[987530]\d|6[987]\d|5[90]\d|42\d|3[875]\d|2[98654321]\d|9[8543210]|8[6421]|6[6543210]|5[87654321]|4[987654310]|3[9643210]|2[70]|7|1)\d{1,14}$
# Starts with +, then some digits which may have spaces, ending with a digit.
INTL_PHONE_PATTERN = r'\+(?:\d[\- ]?){6,14}\d'

# Note that we don't use the \b at the beginning: it's not a \W to \w transition when leading with paren or plus.
PHONE_PATTERN = r'((?:' + US_PHONE_PATTERN + r'|' + INTL_PHONE_PATTERN + r'))\b'

# http://blog.stevenlevithan.com/archives/validate-phone-number
POSSIBLE_PHONE_PATTERN = r'(\+?\b[\d][0-9 \(\).\-]{8,}[\d])\b'


COMPILED_PHONE_PATTERN = re.compile(PHONE_PATTERN, re.VERBOSE)
COMPILED_POSSIBLE_PHONE_PATTERN = re.compile(POSSIBLE_PHONE_PATTERN, re.VERBOSE)

def find_phone_numbers(text, context=20):
    return find_all_matches(COMPILED_PHONE_PATTERN, text, "PHONE_NUMBER", context)

def find_possible_phone_numbers(text, context=20):
    return find_all_matches(COMPILED_POSSIBLE_PHONE_PATTERN, text, "POSSIBLE_PHONE_NUMBER", context)


#####################
# Personal context
#####################

EMAIL_CONTEXT = re.compile(
    r'\b(my (?:personal )?e[\- ]?mail|e[\- ]mail me|e[\- ]mail(?: address)?|send e[\- ]mail|write me|talk with me|Skype|address|facebook)\b',
    re.I
)

def find_email_context(text, context=20):
    return find_all_matches(EMAIL_CONTEXT, text, "EMAIL_CONTEXT", context)

NAME_CONTEXT = re.compile(
    r'\b(hi|hello|sincerely|yours truly|Dear|Mr|Ms|Mrs|regards|cordially|best wishes|cheers|my name)\b',
    re.I
)

def find_name_context(text, context=20):
    return find_all_matches(NAME_CONTEXT, text, "NAME_CONTEXT", context)

PHONE_CONTEXT = re.compile(
    r'(\bphone:|\bp:|b\c:|\bcall me\b|\(home\)|\(cell\)|my phone|phone number)',
    re.I
)

def find_phone_context(text, context=20):
    return find_all_matches(PHONE_CONTEXT, text, "PHONE_CONTEXT", context)

#####################
# Facebook
#####################

# https://www.facebook.com/user.name
FACEBOOK_PATTERN = re.compile(
    r'\b(https:\/\/www\.facebook\.com\/[\w.]+)\b',
    re.I
)
def find_facebook(text, context=20):
    return find_all_matches(FACEBOOK_PATTERN, text, "FACEBOOK", context)


#####################
# Zip codes
#####################

# Look for a leading space.
ZIPCODE_PATTERN = r'((?<= )\b\d{5}(?:[-\s]\d{4})?\b)'
COMPILED_ZIPCODE_PATTERN = re.compile(ZIPCODE_PATTERN, re.I)

def find_zipcodes(text, context=20):
    return find_all_matches(COMPILED_ZIPCODE_PATTERN, text, "ZIPCODE", context)


#####################
# EMAIL
#####################

EMAIL_PATTERN = r'((?<=\s)([a-zA-Z0-9\(\.\-]+)[@]([a-zA-Z0-9\.]+)\.(?:edu|com|org)\b)'
ORIG_EMAIL_PATTERN = r'(.*)\s+(([a-zA-Z0-9\(\.\-]+)[@]([a-zA-Z0-9\.]+)(.)(edu|com))\\s*(.*)'
#emailPattern='(.*)\\s+([a-zA-Z0-9\\.]+)\\s*(\\(f.*b.*)?(@)\\s*([a-zA-Z0-9\\.\\s;]+)\\s*(\\.)\\s*(edu|com)\\s+(.*)'
COMPILED_EMAIL_PATTERN = re.compile(EMAIL_PATTERN, re.I)

# http://www.regular-expressions.info/email.html
BASIC_EMAIL_PATTERN = r'\b[a-z0-9!#$%&\'*+\/\=\?\^\_\`\{\|\}\~\-]+(?:\.[a-z0-9!#$%&\'*+\/\=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\b'
# case-insensitive didn't work properly for cap-init cases, so add A-Z explicitly:
# BASIC_EMAIL_PATTERN = r'\b[A-Za-z0-9!#$%&\'*+\/\=\?\^\_\`\{\|\}\~\-]+(?:\.[A-Za-z0-9!#$%&\'*+\/\=?^_`{|}~-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\b'

COMPILED_POSSIBLE_EMAIL_PATTERN = re.compile(BASIC_EMAIL_PATTERN, re.I)

def find_emails(text, context=20):
    return find_all_matches(COMPILED_EMAIL_PATTERN, text, "ORIG_EMAIL", context)

def find_possible_emails(text, context=20):
    if '@' in text:
        return find_all_matches(COMPILED_POSSIBLE_EMAIL_PATTERN, text, "EMAIL", context)
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

def find_username(text, username, context=20):
    username_pattern = re.compile(
        r'\b({})\b'.format(username),
        re.I
    )
    return find_all_matches(username_pattern, text, "USERNAME", context)


#####################
# user profile => fullname
#####################

# Note the addition of the '$' at the
# end: match() only constrains the beginning.
LEGAL_NAME_PATTERN = re.compile(r"[\w. \-\'\(\)\,\*\"]+$", re.UNICODE)
REJECTED_NAMES = set()
# People use common words in their names.  Sometimes it's just plain text; sometimes jibberish.
# It's hard to tell the difference.  (E.g. "The" appears in Vietnamese-like names.)
STOPWORDS = ['the', 'and', 'can']

def find_user_fullname(text, fullname, context=20):

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
        print u"ERROR:  fullname '{}' contains unexpected characters.".format(fullname).encode('utf-8')
        REJECTED_NAMES.add(fullname)
        return text

    # Strip parentheses and comma and the like, and escape the characters that are
    # legal in names but may have different meanings in regexps (i.e. apostrophe and period).
    # TODO: if a single comma is found, maybe linearize the name by swapping the delimited parts?
    fullname2 = fullname2.replace('"', ' ').replace('*', ' ').replace('(', ' ').replace(')', ' ').replace(',', ' ').replace('-', '\\-').replace("'", "\\'").replace('.', '\\.')
    
    # Create regexp by breaking the name up.  Assume spaces.
    names = fullname2.strip().split()
    if len(names) == 0:
        print u"ERROR:  fullname '{}' contains only whitespace characters.".format(fullname).encode('utf-8')
        REJECTED_NAMES.add(fullname)
        return text

    patterns = []
    # add the whole, then add each individual part if it's long enough.
    patterns.append(u" ".join(names))
    for name in names:
        if len(name) > 2 and name not in STOPWORDS:
            patterns.append(name)

    # Because we're operating with unicode instead of raw strings, make sure that
    # the slashes are escaped.
    fullname_pattern = re.compile(
        u'\\b({})\\b'.format(u"|".join(patterns)),
        re.I
    )
    return find_all_matches(fullname_pattern, text, "FULLNAME", context)


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
