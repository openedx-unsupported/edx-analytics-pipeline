"""
Tasks for performing encryption on export files.
"""
from contextlib import contextmanager
import logging
import tempfile

import gnupg

from edx.analytics.tasks.util.tempdir import make_temp_directory

log = logging.getLogger(__name__)


@contextmanager
def make_encrypted_file(output_file, key_file_targets, recipients=None):
    """
    Creates a file object to be written to, whose contents will afterwards be encrypted.

    Parameters:
        output_file:  a file object, opened for writing.
        key_file_targets: a list of luigi.Target objects defining the gpg public key files to be loaded.
        recipients:  an optional list of recipients to be loaded.  If not specified, uses all loaded keys.
    """
    with make_temp_directory(prefix="encrypt") as temp_dir:
        # Use temp directory to hold gpg keys.
        gpg = gnupg.GPG(gnupghome=temp_dir)
        gpg.encoding = 'utf-8'
        _import_key_files(gpg, key_file_targets)

        # Create a temp file to contain the unencrypted output, in the same temp directory.
        with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False) as temp_input_file:
            temp_input_filepath = temp_input_file.name
            yield temp_input_file

        # Encryption produces a second file in the same temp directory.
        temp_encrypted_filepath = "{filepath}.gpg".format(filepath=temp_input_filepath)
        if recipients is None:
            recipients = [key['keyid'] for key in gpg.list_keys()]
        with open(temp_input_filepath, 'r') as temp_input_file:
            _encrypt_file(gpg, temp_input_file, temp_encrypted_filepath, recipients)
        _copy_file_to_open_file(temp_encrypted_filepath, output_file)


def _import_key_files(gpg_instance, key_file_targets):
    """
    Load key-file targets into the GPG instance.

    This writes files in the home directory of the instance.
    """
    for key_file_target in key_file_targets:
        log.info("Importing keyfile from %s", key_file_target.path)
        with key_file_target.open('r') as gpg_key_file:
            gpg_instance.import_keys(gpg_key_file.read())


def _encrypt_file(gpg_instance, input_file, encrypted_filepath, recipients):
    """Encrypts a given file open for read, and writes result to a file."""
    gpg_instance.encrypt_file(
        input_file,
        recipients,
        always_trust=True,
        output=encrypted_filepath,
        armor=False,
    )


def _copy_file_to_open_file(filepath, output_file):
    """Copies a filepath to a file object already opened for writing."""
    with open(filepath, 'r') as src_file:
        while True:
            transfer_buffer = src_file.read(1024)
            if transfer_buffer:
                output_file.write(transfer_buffer)
            else:
                break
