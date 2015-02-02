

from edx.analytics.tasks.util import eventlog, opaque_key_util


class Event(object):

    def __init__(self, event_str):
        self.from_dict(eventlog.decode_json(event_str))

    def from_dict(self, root):
        for field_name in ('event_type', 'event_source', 'date', 'time'):
            self.field_to_attr(root, field_name)
        self.event = root.get('event', {})
        self.context = root.get('context', {})
        self.username = root.get('username', '').strip()
        self.course_id = self.context.get('course_id')
        if not self.course_id or not opaque_key_util.is_valid_course_id(self.course_id):
            self.course_id = None

    def field_to_attr(self, source, field_name, attr_prefix=''):
        setattr(self, attr_prefix + field_name, source[field_name])


class EnrollmentEvent(Event):

    DEACTIVATED = 'edx.course.enrollment.deactivated'
    ACTIVATED = 'edx.course.enrollment.activated'
    MODE_CHANGED = 'edx.course.enrollment.mode_changed'

    def from_dict(self, root):
        super(EnrollmentEvent, self).from_dict(root)

        if self.event_type not in (self.DEACTIVATED, self.ACTIVATED, self.MODE_CHANGED):
            raise InvalidEventError('Not an enrollment event.')

        for field_name in ('course_id', 'user_id', 'mode'):
            try:
                self.field_to_attr(self.event, field_name, 'target_')
            except KeyError:
                raise InvalidEventError('Required field "{0}" not found in event payload.'.format(field_name))


class InvalidEventError(Exception):
    pass
