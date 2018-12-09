import gzip
import os
import shutil
import tempfile
from contextlib import contextmanager

import gnupg
from jinja2 import Template


@contextmanager
def gzipped_file(file_path):
    with tempfile.NamedTemporaryFile(suffix='.gz') as temp_file:
        gzip_file = gzip.open(temp_file.name, 'wb')
        try:
            with open(file_path, 'r') as input_file:
                shutil.copyfileobj(input_file, gzip_file)
        finally:
            gzip_file.close()

        temp_file.flush()

        yield temp_file.name


@contextmanager
def template_rendered_file(file_path, context=None):
    with tempfile.NamedTemporaryFile() as temp_file:
        with open(file_path, 'r') as input_file:
            template = Template(input_file.read())
            if context is None:
                context = {}
            temp_file.write(template.render(**context))

        temp_file.flush()

        yield temp_file.name


def decrypt_file(encrypted_filename, decrypted_filename, key_filename='insecure_secret.key'):
    """
    Decrypts an encrypted file.

    Arguments:
        encrypted_filename (str): The full path to the PGP encrypted file.
        decrypted_filename (str): The full path of the the file to write the decrypted data to.
        key_filename (str): The name of the key file to use to decrypt the data. It should correspond to one of the
            keys found in the gpg-keys directory.

    """
    gpg_home_dir = tempfile.mkdtemp()
    try:
        gpg = gnupg.GPG(gnupghome=gpg_home_dir)
        gpg.encoding = 'utf-8'
        with open(os.path.join('gpg-keys', key_filename), 'r') as key_file:
            gpg.import_keys(key_file.read())

        with open(encrypted_filename, 'r') as encrypted_file:
            gpg.decrypt_file(encrypted_file, output=decrypted_filename)
    finally:
        shutil.rmtree(gpg_home_dir)


def decompress_file(compressed_filepath, decompressed_output_filepath):
    with gzip.open(compressed_filepath, 'rb') as compressed_file:
        with open(decompressed_output_filepath, 'wb') as decompressed_file:
            try:
                decompressed_file.write(compressed_file.read())
            finally:
                compressed_file.close()
                decompressed_file.close()
