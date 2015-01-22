
import re

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
