"""
Main method for running tasks on a local machine.

  Invoke a task by running `launch-task` with task's classname and
  arguments for Luigi and for the task.  Use `remote-task` to run
  to submit the task to run on an EMR cluster.

"""

from contextlib import contextmanager
import logging
import os

import boto
import filechunkio
import cjson
import ciso8601
import opaque_keys
import bson
import pyinstrument
import stevedore
import requests

import luigi
import luigi.configuration
import luigi.hadoop

# Tell urllib3 to switch the ssl backend to PyOpenSSL.
# see https://urllib3.readthedocs.org/en/latest/security.html#pyopenssl
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()


log = logging.getLogger(__name__)

OVERRIDE_CONFIGURATION_FILE = 'override.cfg'


def main():
    # In order to see errors during extension loading, you can uncomment the next line.
    logging.basicConfig(level=logging.DEBUG)

    # Load tasks configured using entry_points
    # TODO: launch tasks by their entry_point name
    stevedore.ExtensionManager('edx.analytics.tasks')

    configuration = luigi.configuration.get_config()
    if os.path.exists(OVERRIDE_CONFIGURATION_FILE):
        log.debug('Using %s', OVERRIDE_CONFIGURATION_FILE)
        configuration.add_config_path(OVERRIDE_CONFIGURATION_FILE)
    else:
        log.debug('Configuration file %s does not exist', OVERRIDE_CONFIGURATION_FILE)

    # Tell luigi what dependencies to pass to the Hadoop nodes
    # - boto is used for all direct interactions with s3.
    # - cjson is used for all parsing event logs.
    # - filechunkio is used for multipart uploads of large files to s3.
    # - opaque_keys is used to interpret serialized course_ids
    #   - opaque_keys extensions:  ccx_keys
    #   - dependencies of opaque_keys:  bson, stevedore
    luigi.hadoop.attach(boto, cjson, filechunkio, opaque_keys, bson, stevedore, ciso8601, requests)

    if configuration.getboolean('ccx', 'enabled', default=False):
        import ccx_keys
        luigi.hadoop.attach(ccx_keys)

    # TODO: setup logging for tasks or configured logging mechanism

    # Launch Luigi using the default builder

    with profile_if_necessary(os.getenv('WORKFLOW_PROFILER', ''), os.getenv('WORKFLOW_PROFILER_PATH', '')):
        luigi.run()


@contextmanager
def profile_if_necessary(profiler_name, file_path):
    if profiler_name == 'pyinstrument':
        profiler = pyinstrument.Profiler(use_signal=False)
        profiler.start()

    try:
        yield
    finally:
        if profiler_name == 'pyinstrument':
            profiler.stop()
            profiler.save(filename=os.path.join(file_path, 'launch-task.trace'))


if __name__ == '__main__':
    main()
