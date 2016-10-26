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

    def any_mixed_case(self, pattern, line):
        for match in re.finditer(pattern, line, re.IGNORECASE):
            if not re.search(pattern, match.group(0)):
                return True
        return False

    def mapper(self, line):

        if re.search(r'/c4x/[^/"]+/[^/"]+/[^/"]+/\w+(@\w+)?/', line):
            yield ('unterminated asset locator', line)

        if self.any_mixed_case(r"version@", line):
            yield ("Mixed case 'version'", line)

        if self.any_mixed_case(r"branch@", line):
            yield ("Mixed case 'branch'", line)

        if self.any_mixed_case(r"[ic]4x://", line):
            yield ("Mixed case 'i4x' and 'c4x'", line)

        if self.any_mixed_case(r"[ic]4x:;_;_", line):
            yield ("Mixed case 'i4x' and 'c4x'", line)

        if self.any_mixed_case(r"[ic]4x:%2F%2F", line):
            yield ("Mixed case 'i4x' and 'c4x'", line)

        if self.any_mixed_case(r"\b[ic]4x/\w+/\w+/\w+/\w+", line):
            yield ("Mixed case 'i4x' and 'c4x'", line)

        if self.any_mixed_case(r"\b[ic]4x;_\w+;_\w+;_\w+;_\w+", line):
            yield ("Mixed case 'i4x' and 'c4x'", line)

        if self.any_mixed_case(r"\b[ic]4x%2F\w+%2F\w+%2F\w+%2F\w+", line):
            yield ("Mixed case 'i4x' and 'c4x'", line)

        if re.search(r'"event_type": "(([^:]+:[^/]+)|(i4x://)|(\W[ic]4x/))(%0A|%0D|\\n|\\r)', line):
            yield ("Encoded newlines", line)

        if re.search(r'i4x:;_(?!;_)|(?<!;_)c4x;_', line):
            yield ("Missing '/' characters", line)

        if re.search(r'i4x:%2F(?!%2F)|(?<!%2F)c4x%2F', line):
            yield ("Missing '/' characters", line)

        if re.search(r'i4x:/[^/]|(?<!/)c4x/', line):
            yield ("Missing '/' characters", line)

        if re.search(r'(;_)?i4x;_', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r';_?i4x;_', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r'%2F?i4x%2F', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r'/?i4x/', line):
            yield ("Invalid form of i4x://", line)

        if re.search(r'''[^\W_]\wi4x:;_''', line):
            yield ("i4x:// misspellings", line)

        if re.search(r'''[^\W_]i4x:%2F''', line):
            yield ("i4x:// misspellings", line)

        if re.search(r'''[^\W_]i4x:/''', line):
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
