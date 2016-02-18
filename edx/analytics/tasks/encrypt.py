"""
Tasks for performing encryption on export files.
"""
from contextlib import contextmanager
import logging
import tempfile

import gnupg

from edx.analytics.tasks.util.tempdir import make_temp_directory
from edx.analytics.tasks.util.file_util import copy_file_to_file


log = logging.getLogger(__name__)
key_cache = {}  # pylint: disable=invalid-name


def get_key_from_target(key_file_target):
    """Get the contents of the key file pointed to by the target"""

    # Use a key cache since this is often called from reduce tasks which are executed within the same python process if
    # JVM reuse is enabled.
    key_file_path = key_file_target.path

    key_content = key_cache.get(key_file_path)
    if not key_content:
        with key_file_target.open('r') as gpg_key_file:
            log.info("Reading keyfile from %s", key_file_path)
            key_content = gpg_key_file.read()
        key_cache[key_file_path] = key_content
    else:
        log.info("Using cached keyfile for %s", key_file_path)

    return key_content


@contextmanager
def make_encrypted_file(output_file, key_file_targets, recipients=None, progress=None, dir=None):
    """
    Creates a file object to be written to, whose contents will afterwards be encrypted.

    Parameters:
        output_file:  a file object, opened for writing.
        key_file_targets: a list of luigi.Target objects defining the gpg public key files to be loaded.
        recipients:  an optional list of recipients to be loaded.  If not specified, uses all loaded keys.
        progress:  a function that is called periodically as progress is made.
    """
    with make_temp_directory(prefix="encrypt", dir=dir) as temp_dir:
        # Use temp directory to hold gpg keys.
        gpg = gnupg.GPG(gnupghome=temp_dir)
        gpg.encoding = 'utf-8'
        _import_key_files(gpg, key_file_targets)

        # Create a temp file to contain the unencrypted output, in the same temp directory.
        with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False) as temp_input_file:
            temp_input_filepath = temp_input_file.name
            log.info('Writing data to temporary file: %s', temp_input_filepath)
            yield temp_input_file

        # Encryption produces a second file in the same temp directory.
        temp_encrypted_filepath = "{filepath}.gpg".format(filepath=temp_input_filepath)
        if recipients is None:
            recipients = [key['keyid'] for key in gpg.list_keys()]
        with open(temp_input_filepath, 'r') as temp_input_file:
            _encrypt_file(gpg, temp_input_file, temp_encrypted_filepath, recipients)
        with open(temp_encrypted_filepath) as temp_encrypted_file:
            copy_file_to_file(temp_encrypted_file, output_file, progress)


def _import_key_files(gpg_instance, key_file_targets):
    """
    Load key-file targets into the GPG instance.

    This writes files in the home directory of the instance.
    """
    for key_file_target in key_file_targets:
        log.info("Importing keyfile from %s", key_file_target.path)
        gpg_instance.import_keys(get_key_from_target(key_file_target))


def _encrypt_file(gpg_instance, input_file, encrypted_filepath, recipients):
    """Encrypts a given file open for read, and writes result to a file."""
    log.info('Generating encrypted file: %s', encrypted_filepath)
    gpg_instance.encrypt_file(
        input_file,
        recipients,
        always_trust=True,
        output=encrypted_filepath,
        armor=False,
    )
    log.info('Encryption complete.')
