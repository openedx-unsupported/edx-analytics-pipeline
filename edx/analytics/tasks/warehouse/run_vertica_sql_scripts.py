"""
Support for running multiple SQL scripts against an HP Vertica database in a deterministic fashion.
"""
import yaml
import datetime
import logging
from os import path

import luigi
import luigi.configuration
from edx.analytics.tasks.url import ExternalURL
from edx.analytics.tasks.run_vertica_sql_script import BaseVerticaSqlScriptTaskMixin, RunVerticaSqlScriptTask


log = logging.getLogger(__name__)


class RunVerticaSqlScriptsTaskMixin(BaseVerticaSqlScriptTaskMixin):
    """
    Parameters for running multiple SQL scripts against an HP Vertica database in a deterministic fashion.
    """
    script_configuration = luigi.Parameter(
        description='Path to the configuration file that specifies which scripts to run.'
    )
    script_root = luigi.Parameter(
        default='',
        description='Root directory from which the script locations in the configuration '
        'are referenced from.'
    )


class RunVerticaSqlScriptsTask(RunVerticaSqlScriptsTaskMixin, luigi.WrapperTask):
    """
    A wrapper task for running multiple SQL scripts against an HP Vertica database in a deterministic fashion.

    We use a YAML file that defines a list of scripts to run.  We run the scripts in the order they are defined.
    By using RunVerticaSqlScriptTask, each script stores its own marker, thus allowing us to idempotently run this
    task until all required tasks (aka our scripts) have successfully run for a given date.
    """
    downstream_task = None

    def requires(self):
        return self.get_downstream_task()

    def validate_script_entry(self, script):
      # It has to be a dictionary.
      if not isinstance(script, dict):
        return False

      # It needs to have a name and a script location.
      for attr in ['name', 'location']:
        if attr not in script:
          return False

      return True

    def get_downstream_task(self):
        # If no downstream task has been set, load our configuration and generate our tasks and dependency chain.
        if self.downstream_task is None:
            script_conf_target = ExternalURL(url=self.script_configuration).output()
            with script_conf_target.open('r') as script_conf_file:
                config = yaml.safe_load(script_conf_file)
                if config is not None and isinstance(config, dict):
                    previous_task = None

                    scripts = config.get('scripts', [])

                    # Iterate over the list of scripts in the configuration file in reverse order.  We also zip a list of integers,
                    # representing the zero-based index position of the given script in the overall list.  We iterate in reverse
                    # in order to link each task together, using requires(), to ensure that tasks run sequentially, and in the intended
                    # order: from the top of the file, downwards.
                    for script in scripts:
                        if not self.validate_script_entry(script):
                            log.warn("encountered invalid script entry!")
                            continue

                        new_task = RunVerticaSqlScriptTask(
                            credentials=self.credentials, schema=self.schema, marker_schema=self.marker_schema,
                            date=self.date, read_timeout=self.read_timeout, source_script=path.join(self.script_root, script['location']),
                            script_name=script.get('name'))

                        # If we previously configured a task, set it as a dependency of this one, so it runs prior to.
                        if previous_task is not None:
                            new_task.add_dependency(previous_task)

                        # Mark this as the previously-created task.
                        previous_task = new_task

                    self.downstream_task = previous_task

        # If a downstream task has been set, yield it, triggering Luigi to schedule our scripts.
        if self.downstream_task is not None:
            yield self.downstream_task
