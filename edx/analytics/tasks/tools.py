import logging
import os

import luigi

from edx.analytics.tasks.mapreduce import MapReduceJobTask
from edx.analytics.tasks.url import ExternalURL, get_target_from_url, url_path_join

log = logging.getLogger(__name__)


class OutputDiffTask(MapReduceJobTask):

    left_path = luigi.Parameter()
    right_path = luigi.Parameter()
    group_by = luigi.Parameter()
    output_root = luigi.Parameter()

    enable_direct_output = True

    def init_local(self):
        self.field_indexes = [int(x.strip()) for x in self.group_by.split(',')]

    def requires(self):
        yield ExternalURL(self.left_path)
        yield ExternalURL(self.right_path)

    def mapper(self, line):
        bare_line = line.rstrip('\r\n')
        split_line = bare_line.split('\t')
        key = []
        for index in self.field_indexes:
            key.append(unicode(split_line[index]).encode('utf8'))

        input_file_path = os.environ['map_input_file']
        base_path = (input_file_path.split('/')[:-1]).join('/')
        if self.left_path == base_path:
            from_path = 'left'
        elif self.right_path == base_path:
            from_path = 'right'
        else:
            raise RuntimeError('Unable to figure out which path this record came from: ')

        yield tuple(key), (from_path, bare_line)

    def reducer(self, key, values):
        value_list = []
        for value in values:
            value_list.append(value)

        if len(value_list) > 2:
            log.error('More than two values map to the key {k}: {v}'.format(k=repr(key), v=repr(value_list)))
            self.incr_counter('Comparison', 'Failure - excess values', 1)

        left_value = None
        right_value = None
        if len(value_list) == 1:
            from_path, value = value_list[0]
            self.incr_counter('Comparison', 'Failure - {0} only'.format(from_path), 1)
            if from_path == 'left':
                sign = '< '
            else:
                sign = '> '

            yield (sign + value,)
        else:
            failed = False
            for from_path, value in value_list:
                if from_path == 'left':
                    if left_value is None:
                        left_value = value
                    else:
                        failed = True
                        self.incr_counter('Comparison', 'Failure - excess values: left', 1)
                        log.error('Multiple values came from the same file for {k}: {v0}, {v1}'.format(
                            k=repr(key),
                            v0=repr(left_value),
                            v1=repr(value)
                        ))
                else:
                    if right_value is None:
                        right_value = value
                    else:
                        failed = True
                        self.incr_counter('Comparison', 'Failure - excess values: right', 1)
                        log.error('Multiple values came from the same file for {k}: {v0}, {v1}'.format(
                            k=repr(key),
                            v0=repr(right_value),
                            v1=repr(value)
                        ))

            if not failed and left_value is not None and right_value is not None:
                if left_value != right_value:
                    self.incr_counter('Comparison', 'Failure - differ', 1)
                    yield ('< ' + left_value,)
                    yield ('> ' + right_value,)
                else:
                    self.incr_counter('Comparison', 'match', 1)

    def output(self):
        return get_target_from_url(self.output_root)

    def complete(self):
        return get_target_from_url(url_path_join(self.output_root, '_SUCCESS')).exists()

    def run(self):
        output_target = self.output()
        if not self.complete() and output_target.exists():
            output_target.remove()

        super(OutputDiffTask, self).run()