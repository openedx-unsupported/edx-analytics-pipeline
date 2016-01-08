"""
Utility methods interact with files.
"""
import logging

TRANSFER_BUFFER_SIZE = 1024 * 1024  # 1 MB
log = logging.getLogger(__name__)


class FileCopyMixin(object):
    """Task to copy input file to output."""

    def run(self):
        """Copies input to output using using a fixed buffer."""

        def report_progress(num_bytes):
            """Update hadoop counters as the file is written"""
            self.incr_counter('FileCopyTask', 'Bytes Written to Output', num_bytes)

        if isinstance(self.input(), list):
            if len(self.input()) == 1:
                input_target = self.input()[0]
            else:
                raise ValueError("Number of input files should be exactly 1")
        else:
            input_target = self.input()

        with self.output().open('w') as output_file:
            with input_target.open('r') as input_file:
                copy_file_to_file(input_file, output_file, progress=report_progress)


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
                except:  # pylint: disable=bare-except
                    pass
        else:
            break
    log.info('Copy to output complete')
