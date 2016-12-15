"""
Ensure we can write to Vertica data sources.
"""
from __future__ import absolute_import

import textwrap
from os import path

import luigi
import luigi.task

from mock import sentinel

from edx.analytics.tasks.run_vertica_sql_scripts import RunVerticaSqlScriptsTask
from edx.analytics.tasks.tests import unittest


class RunVerticaSqlScriptsTaskTest(unittest.TestCase):
    """
    Ensure we can run SQL scripts that read and write data to Vertica data sources.
    """

    def create_task(self, credentials='', script_config=''):
        """
        Emulate execution of a generic RunVerticaSqlScriptsTask.
        """
        # Make sure to flush the instance cache so we create a new task object.
        luigi.task.Register.clear_instance_cache()
        task = RunVerticaSqlScriptsTask(
            credentials=sentinel.ignored,
            script_configuration=script_config,
            script_root='',
        )

        return task

    def get_configured_task_chain_for_config(self, config_path):
        """
        Creates the task based on a given YAML configuration file.
        """
        return self.create_task(script_config=path.join('edx/analytics/tasks/tests/fixtures/sql_scripts', config_path)).get_downstream_task()

    def get_tasks_in_chain(self, chain):
        # Enumerate the chain, revealing whether or not anything was actually generated.
        chain_items = list(chain)

        items = []
        if len(chain_items) == 0:
          return items

        # If we're returning more than one item from the requirements, that means we're risking concurrent
        # task execution, which breaks our desire to serially execute tasks one after the other.
        self.assertEqual(len(chain_items), 1)

        # Pull out the chain of dependent tasks.
        current_item = chain_items[0]
        while current_item is not None:
          items.append(current_item)
          current_item = current_item.depends_on

        return list(reversed(items))

    def test_run_with_empty_configuration(self):
        # It's ... valid YAML, but there just won't be anything there.  No generated tasks.
        chain = self.get_configured_task_chain_for_config('empty_config.yaml')
        tasks = self.get_tasks_in_chain(chain)

        self.assertEqual(len(tasks), 0)

    def test_run_with_jacked_up_configuration(self):
        # Again, this is more like "weird, unexpected content that is still valid."  No generated tasks.
        chain = self.get_configured_task_chain_for_config('nonsense_config.yaml')
        tasks = self.get_tasks_in_chain(chain)

        self.assertEqual(len(tasks), 0)

    def test_run_with_single_script(self):
        # A real configuration.  Should be a single generated task.
        chain = self.get_configured_task_chain_for_config('single_script.yaml')
        tasks = self.get_tasks_in_chain(chain)

        self.assertEqual(len(tasks), 1)

    def test_run_with_two_scripts(self):
        # A real configuration.  Should be two generated tasks: script_two, then script_one.
        chain = self.get_configured_task_chain_for_config('two_scripts.yaml')
        tasks = self.get_tasks_in_chain(chain)

        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].script_name, "script one")
        self.assertEqual(tasks[1].script_name, "script two")

    def test_run_with_four_scripts(self):
        # A real configuration.  Should be four generated tasks: script_four, then script_three, then script_two, then script_one.
        chain = self.get_configured_task_chain_for_config('four_scripts.yaml')
        tasks = self.get_tasks_in_chain(chain)

        self.assertEqual(len(tasks), 4)
        self.assertEqual(tasks[0].script_name, "script one")
        self.assertEqual(tasks[1].script_name, "script two")
        self.assertEqual(tasks[2].script_name, "script three")
        self.assertEqual(tasks[3].script_name, "script four")
