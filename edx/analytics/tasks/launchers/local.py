"""
Main method for running tasks on a local machine.

  Invoke a task by running `launch-task` with task's classname and
  arguments for Luigi and for the task.  Use `remote-task` to run
  to submit the task to run on an EMR cluster.

"""

import os.path
import logging

import boto
import argparse
import filechunkio
import cjson

import luigi
import luigi.configuration
import luigi.hadoop

from stevedore.extension import ExtensionManager

log = logging.getLogger(__name__)

OVERRIDE_CONFIGURATION_FILE = 'override.cfg'


def main():
    # In order to see errors during extension loading, you can uncomment the next line.
    # logging.basicConfig(level=logging.DEBUG)

    # Load tasks configured using entry_points
    # TODO: launch tasks by their entry_point name
    ExtensionManager('edx.analytics.tasks')

    configuration = luigi.configuration.get_config()
    if os.path.exists(OVERRIDE_CONFIGURATION_FILE):
        log.debug('Using override.cfg')
        with open(OVERRIDE_CONFIGURATION_FILE, 'r') as override_file:
            log.debug(override_file.read())
        configuration.add_config_path(OVERRIDE_CONFIGURATION_FILE)
    else:
        log.debug('override.cfg does not exist')

    # Tell luigi what dependencies to pass to the Hadoop nodes
    # - argparse is not included by default in python 2.6, but is required by luigi.
    # - boto is used for all direct interactions with s3.
    # - cjson is used for all parsing event logs.
    # - filechunkio is used for multipart uploads of large files to s3.
    luigi.hadoop.attach(argparse, boto, cjson, filechunkio)

    # TODO: setup logging for tasks or configured logging mechanism

    # Launch Luigi using the default builder
    luigi.run()


if __name__ == '__main__':
    main()
