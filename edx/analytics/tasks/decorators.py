"""
Decorators used by the edx.analytics.tasks classes.

"""


def workflow_entry_point(cls):
    """
    Sets the given class's _category attribute to 'workflow_entry_point'.

    """
    cls.task_category = 'workflow_entry_point'
    return cls
