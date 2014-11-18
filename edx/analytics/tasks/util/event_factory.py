"""Define factory class for creating synthetic events."""
import datetime
import json


class SyntheticEventFactory(object):
    """Creates events from non-event data."""

    def __init__(self, *args, **kwargs):
        super(SyntheticEventFactory, self).__init__()
        # define default values for context:
        self.course_id = kwargs.get('course_id', '')
        self.org_id = kwargs.get('org_id', '')
        self.user_id = kwargs.get('user_id', '')
        # define default values for event:
        self.username = kwargs.get('username', '')
        self.hostname = kwargs.get('host', '')
        self.event_source = kwargs.get('event_source', 'server')
        self.event_type = kwargs.get('event_type', 'UNKNOWN')
        self.timestamp = kwargs.get('timestamp', '2012-01-01T00:00.000')
        self.ip_address = kwargs.get('ip', '')
        self.synthesizer = kwargs.get('synthesizer', 'UNKNOWN')
        self.reason = kwargs.get('reason', '')

    @staticmethod
    def _update_with_kwargs(data_dict, **kwargs):
        """Updates a dict from kwargs only if it modifies a top-level value."""
        for key, value in kwargs.iteritems():
            if key in data_dict:
                data_dict[key] = value

    def _create_event_context(self, **kwargs):
        """Returns context dict with test values."""
        context = {
            "course_id": self.course_id,
            "org_id": self.org_id,
            "user_id": self.user_id,
        }
        self._update_with_kwargs(context, **kwargs)
        return context

    def _create_event_synthesized(self, **kwargs):
        """Returns synthesized dict with test values."""
        synthesized = {
            "created_at": datetime.datetime.utcnow().isoformat(),
            "synthesizer": self.synthesizer,
            "reason": self.reason,
        }
        self._update_with_kwargs(synthesized, **kwargs)
        return synthesized

    def create_event_dict(self, event_data_dict, **kwargs):
        """Create a synthetic event as a dict."""
        # Define default values for event log entry.
        event_dict = {
            "username": self.username,
            "host": self.hostname,
            "event_source": self.event_source,
            "event_type": self.event_type,
            "context": self._create_event_context(**kwargs),
            "time": "{0}+00:00".format(self.timestamp),
            "ip": self.ip_address,
            "event": event_data_dict,
            "agent": "",
            "page": None,
            "synthesized": self._create_event_synthesized(**kwargs),
        }
        self._update_with_kwargs(event_dict, **kwargs)
        return event_dict

    def create_event(self, event_data_dict, **kwargs):
        """Create a synthetic event as a json-encoded dict."""
        return json.dumps(self.create_event_dict(event_data_dict, **kwargs))
