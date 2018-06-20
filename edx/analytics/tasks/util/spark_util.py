"""Support for spark tasks"""
import json
import re

import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.constants import PredicateLabels

PATTERN_JSON = re.compile(r'^.*?(\{.*\})\s*$')


def get_event_predicate_labels(event_type, event_source):
    """
    Creates labels by applying hardcoded predicates to a single event.
    Don't pass whole event row to any spark UDF as it generates a different output than expected
    """
    # We only want the explicit event, not the implicit form.
    # return 'test'

    labels = PredicateLabels.ACTIVE_LABEL

    # task & enrollment events are filtered out by spark later as it speeds up due to less # of records

    if event_source == 'server':
        if event_type == 'problem_check':
            labels += ',' + PredicateLabels.PROBLEM_LABEL

        if event_type.startswith('edx.forum.') and event_type.endswith('.created'):
            labels += ',' + PredicateLabels.POST_FORUM_LABEL

    if event_source in ('browser', 'mobile'):
        if event_type == 'play_video':
            labels += ',' + PredicateLabels.PLAY_VIDEO_LABEL

    return labels


def get_key_value_from_event(event, key, default_value=None):
    """
    Get value from event dict by key
    Pyspark does not support dict.get() method, so this approach seems reasonable
    """
    try:
        default_value = event[key]
    except KeyError:
        pass
    return default_value


def get_event_time_string(event_time):
    """Returns the time of the event as an ISO8601 formatted string."""
    try:
        # Get entry, and strip off time zone information.  Keep microseconds, if any.
        timestamp = event_time.split('+')[0]
        if '.' not in timestamp:
            timestamp = '{datetime}.000000'.format(datetime=timestamp)
        return timestamp
    except Exception:  # pylint: disable=broad-except
        return ''


def filter_event_logs(row, lower_bound_date_string, upper_bound_date_string):
    if row is None:
        return ()
    context = row.get('context', '')
    raw_time = row.get('time', '')
    if not context or not raw_time:
        return ()
    course_id = context.get('course_id', '').encode('utf-8')
    user_id = context.get('user_id', None)
    time = get_event_time_string(raw_time).encode('utf-8')
    ip = row.get('ip', '').encode('utf-8')
    if not user_id or not time:
        return ()
    date_string = raw_time.split("T")[0].encode('utf-8')
    if date_string < lower_bound_date_string or date_string >= upper_bound_date_string:
        return ()  # discard events outside the date interval
    return (user_id, course_id, ip, time, date_string)


def parse_json_event(line, nested=False):
    """
    Parse a tracking log input line as JSON to create a dict representation.
    """
    try:
        parsed = json.loads(line)
    except Exception:
        if not nested:
            json_match = PATTERN_JSON.match(line)
            if json_match:
                return parse_json_event(json_match.group(1), nested=True)
        return None
    return parsed


def load_and_filter_rdd(path):
    from edx.analytics.tasks.util.s3_util import ScalableS3Client
    from pickle import Pickler
    import gzip
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    s3_conn = ScalableS3Client()
    raw_data = StringIO(s3_conn.get_as_string(path))
    gzipfile = gzip.GzipFile(fileobj=raw_data)  # this can be improved
    content_string = gzipfile.read().encode('utf-8')
    return [line for line in content_string.split("\n") if line]


def load_and_filter(spark_session, file, lower_bound_date_string, upper_bound_date_string):
    return spark_session.sparkContext.textFile(file) \
        .map(parse_json_event) \
        .map(lambda row: filter_event_logs(row, lower_bound_date_string, upper_bound_date_string)) \
        .filter(bool)


def validate_course_id(course_id):
    course_id = opaque_key_util.normalize_course_id(course_id)
    if course_id:
        if opaque_key_util.is_valid_course_id(course_id):
            return course_id
    return ''


def get_course_id(event_context, from_url=False):
    """
    Gets course_id from event's data.
    Don't pass whole event row to any spark UDF as it generates a different output than expected
    """
    if event_context == '' or event_context is None:
        # Assume it's old, and not worth logging...
        return ''

    # Get the course_id from the data, and validate.
    course_id = opaque_key_util.normalize_course_id(get_key_value_from_event(event_context, 'course_id', ''))
    if course_id:
        if opaque_key_util.is_valid_course_id(course_id):
            return course_id

    return ''

    # TODO : make it work with url as well
    # Try to get the course_id from the URLs in `event_type` (for implicit
    # server events) and `page` (for browser events).
    # if from_url:
    #     source = get_key_value_from_event(event, 'event_source')
    #
    #     if source == 'server':
    #         url = get_key_value_from_event(event, 'event_type', '')
    #     elif source == 'browser':
    #         url = get_key_value_from_event(event, 'page', '')
    #     else:
    #         url = ''
    #
    #     course_key = opaque_key_util.get_course_key_from_url(url)
    #     if course_key:
    #         return unicode(course_key)
    #
    # return ''
