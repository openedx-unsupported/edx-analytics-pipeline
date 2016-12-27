"""
Test the edx.analytics.tasks.decorators methods
"""

from edx.analytics.tasks.tests import unittest
from edx.analytics.tasks import decorators


class TaskCategoryTest(unittest.TestCase):

    class NoDecoratorClass(object):
        pass

    def test_no_decorator(self):
        self.assertRaises(AttributeError, getattr, self.NoDecoratorClass, 'task_category')

    @decorators.workflow_entry_point
    class WorkflowEntryPointClass(object):
        pass

    def test_workflow_entry_point(self):
        self.assertEquals(getattr(self.WorkflowEntryPointClass, 'task_category'), 'workflow_entry_point')
