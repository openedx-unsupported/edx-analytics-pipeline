"""
Utility methods interact with files.
"""
import logging
import os
import sys
from contextlib import contextmanager

from edx.analytics.tasks.util.url import get_target_from_url

TRANSFER_BUFFER_SIZE = 1024 * 1024  # 1 MB
log = logging.getLogger(__name__)


def copy_file_to_file(src_file, output_file, progress=None):
    """Copies a source file to an output file, both of which are already open."""
    if hasattr(src_file, 'name'):
        log.info('Copying to output: %s', src_file.name)

    while True:
        transfer_buffer = src_file.read(TRANSFER_BUFFER_SIZE)
        if transfer_buffer:
            output_file.write(transfer_buffer)
            if progress:
                try:
                    progress(len(transfer_buffer))
                except Exception:  # pylint: disable=bare-except
                    pass
        else:
            break
    log.info('Copy to output complete')


@contextmanager
def read_config_file(filename):
    """Read a config file from either an external source (S3, HDFS etc) or the "share" directory of this repo."""
    if os.path.basename(filename) != filename:
        target = get_target_from_url(filename)
        with target.open('r') as config_file:
            yield config_file
    else:
        file_path = os.path.join(sys.prefix, 'share', 'edx.analytics.tasks', filename)
        with open(file_path, 'r') as config_file:
            yield config_file
