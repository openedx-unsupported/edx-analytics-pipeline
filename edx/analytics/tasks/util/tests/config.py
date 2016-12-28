"""Support modifying luigi configuration settings in tests."""

from functools import wraps

from luigi.configuration import LuigiConfigParser


def with_luigi_config(*decorator_args):
    """
    Decorator constructor that temporarily overrides a config file setting while executing the method.

    Can be passed a special value of edx.analytics.tasks.util.tests.config.OPTION_REMOVED which will ensure that the given
    option is *not* present in the configuration.

    Examples::

        @with_luigi_config('foo', 'bar', 'baz')
        def test_something(self):
            value = luigi.configuration.get_config().get('foo', 'bar')
            assert value == 'baz'

        @with_luigi_config(('foo', 'bar', 'baz'), ('x', 'y', 'z'))
        def test_something_else(self):
            config = luigi.configuration.get_config()

            value = config.get('foo', 'bar')
            assert value == 'baz'

            other_value = config.get('x', 'y')
            assert other_value == 'z'

        from edx.analytics.tasks.util.tests.config import OPTION_REMOVED

        @with_luigi_config('foo', 'bar', OPTION_REMOVED)
        def test_no_option(self):
            try:
                luigi.configuration.get_config().get('foo', 'bar')
            except NoOptionError:
                # This will always be executed regardless of whether or not a 'foo' section exists in the config files
                # and contains a 'bar' option.
                pass

    """

    def config_decorator(func):
        """Actual function decorator"""

        @wraps(func)
        def function_config_wrapper(*args, **kwargs):
            """Overrides the given settings while executing the wrapped function."""

            # Save the current configuration
            current_config_instance = LuigiConfigParser._instance

            # This will force the configuration to be reloaded from disk.
            LuigiConfigParser._instance = None
            try:
                # Get a brand new configuration object by loading from disk.
                new_instance = LuigiConfigParser.instance()

                def modify_config(section, option, value):
                    if value == OPTION_REMOVED:
                        new_instance.remove_option(section, option)
                    else:
                        new_instance.set(section, option, str(value))

                # Support the single override case: @with_luigi_config('section', 'option', 'value')
                if isinstance(decorator_args[0], basestring):
                    section, option, value = decorator_args
                    modify_config(section, option, value)
                else:
                    # As well as the generic case that allows multiple overrides at once:
                    # @with_luigi_config(('sec', 'opt', 'val'), ('foo', 'bar', 'baz'))
                    for section, option, value in decorator_args:
                        modify_config(section, option, value)

                return func(*args, **kwargs)
            finally:
                # Restore the saved configuration
                LuigiConfigParser._instance = current_config_instance

        return function_config_wrapper

    return config_decorator


OPTION_REMOVED = object()
