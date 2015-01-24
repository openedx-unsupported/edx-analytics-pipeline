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
import opaque_keys
import bson
import pyinstrument
import stevedore

import luigi
import luigi.configuration
import luigi.hadoop


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
        log.debug('Using override.cfg')
        with open(OVERRIDE_CONFIGURATION_FILE, 'r') as override_file:
            log.debug(override_file.read())
        configuration.add_config_path(OVERRIDE_CONFIGURATION_FILE)
    else:
        log.debug('override.cfg does not exist')

    # Tell luigi what dependencies to pass to the Hadoop nodes
    # - boto is used for all direct interactions with s3.
    # - cjson is used for all parsing event logs.
    # - filechunkio is used for multipart uploads of large files to s3.
    # - opaque_keys is used to interpret serialized course_ids
    #   - dependencies of opaque_keys:  bson, stevedore
    luigi.hadoop.attach(boto, cjson, filechunkio, opaque_keys, bson, stevedore)

    # TODO: setup logging for tasks or configured logging mechanism

    # Launch Luigi using the default builder

    with profile_if_necessary(os.getenv('WORKFLOW_PROFILER', '')):
        luigi.run()


@contextmanager
def profile_if_necessary(profiler_name):
    if profiler_name == 'pyinstrument':
        profiler = pyinstrument.Profiler(use_signal=False)
        profiler.start()

    try:
        yield
    finally:
        if profiler_name == 'pyinstrument':
            profiler.stop()
            profiler.save(filename='edx-analytics-pipeline-{pid}.pyinstr.trace'.format(pid=os.getpid()))


if __name__ == '__main__':
    main()
