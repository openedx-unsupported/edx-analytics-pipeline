
import re

import cjson
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url


class Grep(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()
    search = luigi.Parameter(is_list=True)

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        for p in self.search:
            if re.search(p, line):
                yield (line,)
                break

    def output(self):
        return get_target_from_url(self.output_root)


class FindUsers(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):
        value = self.get_event_and_date_string(line)
        if value is None:
            return
        event, date_string = value

        if event.get('event_type', '') != '/account/login/':
            return

        payload_str = event.get('event')
        if not payload_str:
            return

        try:
            payload = cjson.decode(payload_str)
        except:
            return

        get_dict = payload.get('GET', {})
        if 'password' not in get_dict:
            return

        emails = get_dict.get('email')
        if not emails:
            return

        for email in emails:
            yield (date_string, email)

    def output(self):
        return get_target_from_url(self.output_root)
