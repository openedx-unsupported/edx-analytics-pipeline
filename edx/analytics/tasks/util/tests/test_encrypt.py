"""Tests of utilities to encrypt files."""

import tempfile
from unittest import TestCase

import gnupg

from edx.analytics.tasks.util.encrypt import _import_key_files, make_encrypted_file
from edx.analytics.tasks.util.tempdir import make_temp_directory
from edx.analytics.tasks.util.url import get_target_from_url, url_path_join


class MakeEncryptedFileTest(TestCase):
    """Test make_encrypted_file context manager."""

    def setUp(self):
        self.recipient = 'daemon@edx.org'
        self.gpg_key_dir = 'gpg-keys'
        self.recipient_private_key = 'insecure_secret.key'
        self.key_file_targets = [get_target_from_url(url_path_join(self.gpg_key_dir, self.recipient))]

    def get_decrypted_data(self, input_file, key_file_target):
        """Decrypts contents of input, and writes to output file object open for writing."""
        with make_temp_directory(prefix="decrypt") as temp_dir:
            # Use temp directory to hold gpg keys.
            gpg_instance = gnupg.GPG(gnupghome=temp_dir)
            _import_key_files(gpg_instance, [key_file_target])
            decrypted_data = gpg_instance.decrypt_file(input_file, always_trust=True)
            return decrypted_data

    def check_encrypted_data(self, encrypted_file, expected_values):
        """Decrypt the file and compare against expected values."""
        key_file_target = get_target_from_url(url_path_join(self.gpg_key_dir, self.recipient_private_key))
        decrypted_data = self.get_decrypted_data(encrypted_file, key_file_target)
        self.assertEquals(str(decrypted_data).strip().split('\n'), expected_values)

    def test_make_encrypted_file(self):
        values = ['this', 'is', 'a', 'test']
        with tempfile.NamedTemporaryFile() as output_file:
            with make_encrypted_file(output_file, self.key_file_targets, [self.recipient]) as encrypted_output_file:
                for value in values:
                    encrypted_output_file.write(value)
                    encrypted_output_file.write('\n')

            output_file.seek(0)
            self.check_encrypted_data(output_file, values)

    def test_make_encrypted_file_with_implied_recipients(self):
        values = ['this', 'is', 'a', 'test']
        with tempfile.NamedTemporaryFile() as output_file:
            with make_encrypted_file(output_file, self.key_file_targets) as encrypted_output_file:
                for value in values:
                    encrypted_output_file.write(value)
                    encrypted_output_file.write('\n')

            output_file.seek(0)
            self.check_encrypted_data(output_file, values)
