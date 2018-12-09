"""Support for reading tracking event logs."""

import datetime
import logging
import re

import cjson

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util

log = logging.getLogger(__name__)

PATTERN_JSON = re.compile(r'^.*?(\{.*\})\s*$')


def decode_json(line):
    """Wrapper to decode JSON string in an implementation-independent way."""
    # TODO: Verify correctness of cjson
    return cjson.decode(line)


def encode_json(obj):
    """Wrapper to re-encode JSON string in an implementation-independent way."""
    # TODO: Verify correctness of cjson
    return cjson.encode(obj)


def parse_json_event(line, nested=False):
    """
    Parse a tracking log input line as JSON to create a dict representation.

    Arguments:
    * line:  the eventlog text
    * nested: boolean flag permitting this to be called recursively.

    Apparently some eventlog entries are pure JSON, while others are
    JSON that are prepended by a timestamp.
    """
    try:
        parsed = decode_json(line)
    except Exception:
        if not nested:
            json_match = PATTERN_JSON.match(line)
            if json_match:
                return parse_json_event(json_match.group(1), nested=True)

        # TODO: There are too many to be logged.  It might be useful
        # at some point to collect stats on the length of truncation
        # and the counts for different event "names" (normalized
        # event_type values).

        # Note that empirically some seem to be truncated in input
        # data at 10000 characters, 2043 for others...
        return None

    # TODO: add basic validation here.

    return parsed


def parse_json_server_event(line, requested_event_type):
    """
    Parse a tracking log input line as JSON to create a dict representation.

    Arguments:
        line:  the eventlog text
        requested_event_type: string representing the requested event_type

    Returns:
        tracking event log entry as a dict, if line corresponds to a server
            event with the requested event_type.

    Returns None if an error is encountered or if it doesn't match.
    """
    # Before parsing, check that the line contains something that
    # suggests it's a problem_check event.
    if requested_event_type not in line:
        return None

    # Parse the line into a dict.
    event = parse_json_event(line)
    if event is None:
        # The line didn't parse.  We know that some significant number
        # of server lines do not parse because of line length issues,
        # so log these here.
        log.error("encountered event line that did not parse: %s", line)
        return None

    # We are only interested in server events, not browser events.
    event_source = event.get('event_source')
    if event_source is None:
        log.error("encountered event with no event_source: %s", event)
        return None
    if event_source != 'server':
        return None

    # We only want the explicit event, not the implicit form.
    event_type = event.get('event_type')
    if event_type is None:
        log.error("encountered event with no event_type: %s", event)
        return None
    if event_type != requested_event_type:
        return None

    return event


# Time-related terminology:
# * datetime: a datetime object.
# * timestamp: a string, with date and time (to millisecond), in ISO format.
# * datestamp: a string with only date information, in ISO format.

def datetime_to_timestamp(datetime_obj):
    """
    Returns a string with the datetime value of the provided datetime object.

    Note that if the datetime has zero microseconds, the microseconds will not be output.
    """
    return datetime_obj.isoformat()


def datetime_to_datestamp(datetime_obj):
    """Returns a string with the date value of the provided datetime object."""
    return datetime_obj.strftime('%Y-%m-%d')


def timestamp_to_datestamp(timestamp):
    """Returns a string with the date value of the provided ISO datetime string."""
    return timestamp.split('T')[0]


def get_event_time(event):
    """Returns a datetime object from an event object, if present."""
    try:
        return datetime.datetime.strptime(get_event_time_string(event), '%Y-%m-%dT%H:%M:%S.%f')
    except Exception:  # pylint: disable=broad-except
        return None


def get_event_username(event):
    """Returns a username from an event object, if present."""
    username = event.get('username')
    # Some usernames have trailing newlines, so remove that.
    if username is not None:
        username = username.strip()
        if len(username) == 0:
            username = None
    return username


def get_event_time_string(event):
    """Returns the time of the event as an ISO8601 formatted string."""
    try:
        # Get entry, and strip off time zone information.  Keep microseconds, if any.
        raw_timestamp = event['time']
        timestamp = raw_timestamp.split('+')[0]
        if '.' not in timestamp:
            timestamp = '{datetime}.000000'.format(datetime=timestamp)
        return timestamp
    except Exception:  # pylint: disable=broad-except
        return None


def get_event_data(event):
    """
    Returns event data from an event log entry as a dict object.

    Returns None if not found.
    """
    event_value = event.get('event')

    if event_value is None:
        log.error("encountered event with missing event value: %s", event)
        return None

    if event_value == '':
        # Note that this happens with some browser events.  Instead of
        # failing to parse it as a JSON string, just return an empty dict.
        return {}

    if isinstance(event_value, basestring):
        if len(event_value) == 512 and 'POST' in event_value:
            # It's an implicit event with a truncated JSON string, as generated by tracking middleware.
            log.debug("encountered implicit event with truncated event value: %s", event)
            return None
        elif '{' not in event_value and '=' in event_value:
            # It's a key-value pair, as is generated for example by the 'problem_check' browser event.
            # For now, skip it.
            log.debug("encountered event with string of key-value pairs. event value: %s", event)
            return None
        # If the value is a string, try to parse as JSON into a dict.
        try:
            event_value = decode_json(event_value)
        except Exception:
            log.error("encountered event with unparsable event value: %s", event)
            return None
    elif isinstance(event_value, list):
        # This is currently produced by the 'problem_graded' browser event.
        # Since it doesn't produce a dict, we will continue to reject it, until
        # we're clear we know how to process it.
        log.debug("encountered event with value of type list. event value: %s", event)
        return None

    if isinstance(event_value, dict):
        # It's fine, just return.
        return event_value
    else:
        log.error("encountered event data with unrecognized type: %s", event)
        return None


def get_augmented_event_data(event, fields_to_augment):
    """
    Returns event data from an event log entry, and adds additional fields.

    Args:
        event: event log entry as a dict object
        fields_to_augment: list of field names to use as keys.

    Returns:
        dict containing event data, with keys listed in `fields_to_augment`
            pulled from event dict and place in the returned dict.

    Returns None if not found.
    """
    # Get the event data.
    event_data = get_event_data(event)
    if event_data is None:
        # Assume it's already logged (and with more specifics).
        return None

    if 'timestamp' in fields_to_augment:
        # Get the timestamp as an object.
        datetime_obj = get_event_time(event)
        if datetime_obj is None:
            log.error("encountered event with bad datetime: %s", event)
            return None
        timestamp = datetime_to_timestamp(datetime_obj)
        event_data['timestamp'] = timestamp

    if 'context' in fields_to_augment:
        # Get the event context.
        context = event.get('context')
        if context is None:
            # Too common -- do not log here.
            return None
        event_data['context'] = context

    if 'username' in fields_to_augment:
        username = event.get('username')
        if username is None:
            log.error("encountered event with unexpected missing username: %s", event)
            return None
        event_data['username'] = username

    return event_data


def get_course_id(event, from_url=False):
    """Gets course_id from event's data."""

    # Get the event data:
    event_context = event.get('context')
    if event_context is None:
        # Assume it's old, and not worth logging...
        return None

    # Get the course_id from the data, and validate.
    course_id = opaque_key_util.normalize_course_id(event_context.get('course_id', ''))
    if course_id:
        if opaque_key_util.is_valid_course_id(course_id):
            return course_id
        else:
            log.error("encountered event with bogus course_id: %s", event)
            return None

    # Try to get the course_id from the URLs in `event_type` (for implicit
    # server events) and `page` (for browser events).
    if from_url:
        source = event.get('event_source')

        if source == 'server':
            url = event.get('event_type', '')
        elif source == 'browser':
            url = event.get('page', '')
        else:
            url = ''

        course_key = opaque_key_util.get_course_key_from_url(url)
        if course_key:
            return unicode(course_key)

    return None
