
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

        payload_str = event.get('event')
        if not payload_str:
            return

        try:
            payload = cjson.decode(payload_str)
        except:
            return

        try:
            get_dict = payload['GET']
        except:
            return

        if 'password' not in get_dict:
            return

        emails = get_dict.get('email')
        if not emails:
            return

        try:
            for email in emails:
                yield (email, 1)
        except:
            yield(emails, 1)

    def reducer(self, email, values):
        yield (email,)

    def output(self):
        return get_target_from_url(self.output_root)
