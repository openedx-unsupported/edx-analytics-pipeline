"""
Main method for running tasks on a local machine.

  Invoke a task by running `launch-task` with task's classname and
  arguments for Luigi and for the task.  Use `remote-task` to run
  to submit the task to run on an EMR cluster.

"""

import argparse
from contextlib import contextmanager
import logging
import os
import sys

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

import edx.analytics.tasks

# Tell urllib3 to switch the ssl backend to PyOpenSSL.
# see https://urllib3.readthedocs.org/en/latest/security.html#pyopenssl
import urllib3.contrib.pyopenssl
urllib3.contrib.pyopenssl.inject_into_urllib3()


log = logging.getLogger(__name__)

OVERRIDE_CONFIGURATION_FILE = 'override.cfg'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--additional-config', help='additional configuration file to be loaded after default/override',
        default=None, action='append')
    arguments, _extra_args = parser.parse_known_args()

    # We get a cleaned command-line arguments list, free of the arguments *we* care about, since Luigi will throw
    # errors when it sees arguments that it or the workflow didn't specify.  We pass these in when invoking Luigi.
    cmdline_args = get_cleaned_command_line_args()

    # In order to see errors during extension loading, you can uncomment the next line.
    logging.basicConfig(level=logging.DEBUG)

    # Load tasks configured using entry_points
    # TODO: launch tasks by their entry_point name
    stevedore.ExtensionManager('edx.analytics.tasks')

    # Load the override configuration if it's specified/exists.
    configuration = luigi.configuration.get_config()
    if os.path.exists(OVERRIDE_CONFIGURATION_FILE):
        log.debug('Loading override configuration \'%s\'...', OVERRIDE_CONFIGURATION_FILE)
        configuration.add_config_path(OVERRIDE_CONFIGURATION_FILE)
    else:
        log.debug('Configuration file \'%s\' does not exist!', OVERRIDE_CONFIGURATION_FILE)

    # Load any additional configuration files passed in.
    if arguments.additional_config is not None:
        for additional_config in arguments.additional_config:
            if os.path.exists(additional_config):
                log.debug('Loading additional configuration file \'%s\'...', additional_config)
                configuration.add_config_path(additional_config)
            else:
                log.debug('Configuration file \'%s\' does not exist!', additional_config)


    # Tell luigi what dependencies to pass to the Hadoop nodes
    # - edx.analytics.tasks is used to load the pipeline code, since we cannot trust all will be loaded automatically.
    # - boto is used for all direct interactions with s3.
    # - cjson is used for all parsing event logs.
    # - filechunkio is used for multipart uploads of large files to s3.
    # - opaque_keys is used to interpret serialized course_ids
    #   - opaque_keys extensions:  ccx_keys
    #   - dependencies of opaque_keys:  bson, stevedore
    luigi.hadoop.attach(edx.analytics.tasks)
    luigi.hadoop.attach(boto, cjson, filechunkio, opaque_keys, bson, stevedore, ciso8601, requests)

    if configuration.getboolean('ccx', 'enabled', default=False):
        import ccx_keys
        luigi.hadoop.attach(ccx_keys)

    # TODO: setup logging for tasks or configured logging mechanism

    # Launch Luigi using the default builder

    with profile_if_necessary(os.getenv('WORKFLOW_PROFILER', ''), os.getenv('WORKFLOW_PROFILER_PATH', '')):
        luigi.run(cmdline_args)


def get_cleaned_command_line_args():
  """
  Gets a list of command-line arguments after removing local launcher-specific parameters.
  """
  arg_list = sys.argv[1:]
  modified_arg_list = arg_list

  for i, v in enumerate(arg_list):
    if v == '--additional-config':
      # Clear out the flag, and clear out the value attached to it.
      modified_arg_list[i] = None
      modified_arg_list[i+1] = None

  return list(filter(lambda x: x is not None, modified_arg_list))


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
