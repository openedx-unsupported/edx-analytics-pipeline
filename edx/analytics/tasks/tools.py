import datetime
import logging
import re
import gzip

import cjson
import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask, MultiOutputMapReduceJobTask
from edx.analytics.tasks.pathutil import EventLogSelectionMixin
from edx.analytics.tasks.url import get_target_from_url, url_path_join, ExternalURL
from edx.analytics.tasks.util import eventlog, opaque_key_util


log = logging.getLogger(__name__)


class FindInvalidOpaqueKeys(EventLogSelectionMixin, MapReduceJobTask):

    output_root = luigi.Parameter()

    def mapper(self, line):

        if re.search(r'/c4x/[^/"]+/[^/"]+/[^/"]+/[^@"]+(@[^/"]+)?/', line):
            yield ('asset locator', line)
        if re.search(r"version@", line, re.IGNORECASE) and not re.search(r"version", line):
            yield ("Mixed case 'version'", line)

        if (re.search(r"branch@", line, re.IGNORECASE) and not re.search(r"branch", line)):
            yield ("Mixed case 'branch'", line)

        if (re.search(r"[ic]4x", line, re.IGNORECASE) and not re.search("[ic]4x", line)):
            yield ("Mixed case 'i4x' and 'c4x'", line)

        if re.search(r'"event_type": "[^"]*(%0A|%0D|\\n|\\r)', line):
            yield ("Encoded newlines", line)

        if re.search(r'i4x:;_(?!;_)|(?<!;_)c4x', line):
            yield ("Missing '/' characters", line)

        if re.search(r'i4x:%2F(?!%2F)|(?<!%2F)c4x', line):
            yield ("Missing '/' characters", line)

        if re.search(r'i4x:/[^/]|(?<!/)c4x', line):
            yield ("Missing '/' characters", line)

        if re.search(r'(;_)?i4x;_', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r';_?i4x;_', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r'%2F?i4x%2F', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r'/?i4x/', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r'''(?<![/'"])i4x:;_''', line):
            yield ("i4x:// misspellings", line)

        if re.search(r'''(?<![/'"])i4x:%2F''', line):
            yield ("i4x:// misspellings", line)

        if re.search(r'''(?<![/'"])i4x:/''', line):
            yield ("i4x:// misspellings", line)

        if re.search(r'aside-(usage|def)', line):
            yield ("aside encodings changed", line)

    def reducer(self, key, values):
        count = 0
        for line in values:
            count += 1
            if count < 100:
                yield key, (count, line)
        yield key, (count, "TOTAL COUNT")

    def output(self):
        return get_target_from_url(self.output_root)
