"""Support for spark tasks"""
import edx.analytics.tasks.util.opaque_key_util as opaque_key_util
from edx.analytics.tasks.util.constants import PredicateLabels


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
        else:
            return ''  # we'll filter out empty course since string is expected

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
    return ''
