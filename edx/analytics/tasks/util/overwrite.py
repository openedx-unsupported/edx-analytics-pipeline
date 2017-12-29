"""
Provide support for overwriting existing output files.
"""
import logging

import luigi

log = logging.getLogger(__name__)


class OverwriteOutputMixin(object):
    """
    Provides support to allow a workflow to force execution of a task.

    For tasks that may generate the most current version of a data import,
    we may want the import to be run whenever asked, rather than on a
    schedule with labelled outputs.

    Assumes that the same task object is accessed later, and can
    hold the state.  This assumption may be flawed.  (If the task
    is recreated from its arguments, the new task object doesn't
    contain the state that the task did when it was executed.)

    Note that this should be included in a task definition *before*
    the Task base class, so that the complete() method is overridden.
    """
    overwrite = luigi.BoolParameter(
        default=False,
        description='Whether or not to overwrite existing outputs; set to False by default for now.',
        significant=False
    )
    attempted_removal = False

    def complete(self):
        """
        Wrap Task.complete() to check for overwrite flag.
        """
        # Force complete() to return False any time before the job is
        # actually run, but defer the removal of output until the job is
        # actually run.  This is better than performing the removal
        # at task construction time, since side effects at task
        # definition are less intuitive than having all side effects
        # occur only during execution.
        if self.overwrite and not self.attempted_removal:
            return False
        else:
            return super(OverwriteOutputMixin, self).complete()

    def remove_output_on_overwrite(self):
        """
        Remove output only if it exists and needs to be removed.

        This is a default implementation.  It can be overridden by
        classes that need to do something else to remove output.
        """
        if self.overwrite:
            self.attempted_removal = True
            if self.output().exists():
                log.info("Removing existing output for task %s", str(self))
                self.output().remove()
