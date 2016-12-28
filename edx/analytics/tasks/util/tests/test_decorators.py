"""
Test the edx.analytics.tasks.util.decorators methods
"""

from unittest import TestCase

from edx.analytics.tasks.util import decorators


class TaskCategoryTest(TestCase):

    class NoDecoratorClass(object):
        pass

    def test_no_decorator(self):
        self.assertRaises(AttributeError, getattr, self.NoDecoratorClass, 'task_category')

    @decorators.workflow_entry_point
    class WorkflowEntryPointClass(object):
        pass

    def test_workflow_entry_point(self):
        self.assertEquals(getattr(self.WorkflowEntryPointClass, 'task_category'), 'workflow_entry_point')
