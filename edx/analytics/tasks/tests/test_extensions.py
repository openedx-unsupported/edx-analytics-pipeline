"""
Test for tasks extensions using entry points

"""
from unittest import TestCase

from stevedore.extension import ExtensionManager

from edx.analytics.tasks import EXTENSION_NAMESPACE


class TestExtensions(TestCase):
    def test_extensions_config(self):
        """
        Test if the edx.analytics.tasks entry_points are configured properly.

        Note that extensions are only defined after the package
        `edx.analytics.tasks` has been installed.

        """

        manager = ExtensionManager(
            namespace=EXTENSION_NAMESPACE,
            on_load_failure_callback=self.on_load_failure,
            verify_requirements=True
        )

        if len(manager.extensions) == 0:
            reason = 'no entry_points for {0} namespace'
            raise unittest.SkipTest(reason.format(EXTENSION_NAMESPACE))

    def on_load_failure(self, manager, entrypoint, exception):
        raise exception
