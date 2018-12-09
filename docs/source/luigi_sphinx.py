"""
Helper methods for generating Sphinx documentation for luigi.Task subclasses.
"""
import re
import inspect
import luigi

from luigi.parameter import _no_value

def append_parameters(_app, _what, _name, obj, _options, lines):
    """
    Sphinx extension for appending a luigi.Task class's luigi.Parameter attributes
    to the class documentation as "parameters".

    * Uses the luigi.Parameter.description field to describe the attribute.
    * Marks parameters with default values as `optional`, and displays the default value
      (unless there is mention of a default already in the description).
    * Marks `insignificant` parameters.

    """
    default_re = re.compile(r'default', flags=re.IGNORECASE)
    if inspect.isclass(obj) and issubclass(obj, luigi.Task):
        members = inspect.getmembers(obj)
        for (membername, membervalue) in members:
            if isinstance(membervalue, luigi.Parameter):
                param = {
                    'name': membername,
                    'type': membervalue.__class__.__name__,
                    'description': '',
                }
                if membervalue.description is not None:
                    param['description'] = membervalue.description

                    # Append a full stop, for consistency.
                    if not param['description'].endswith('.'):
                        param['description'] = '{description}.'.format(description=param['description'])

                # Mark configured parameters (requires protected-access)
                # pylint: disable=W0212
                if hasattr(membervalue, '_config_path') and membervalue._config_path is not None:
                    param['default'] = 'pulled from ``{section}.{name}``'.format(**membervalue._config_path)
                    param['type'] = u'{type}, configurable'.format(**param)

                # Mark optional parameters
                elif hasattr(membervalue, '_default') and membervalue._default != _no_value:
                    param['default'] = membervalue._default
                    param['type'] = u'{type}, optional'.format(**param)

                if 'default' in param:
                    # Show default value, if not already in the description.
                    # NB: This test is useful to avoid redundant descriptions,
                    # and for dynamically determined defaults like date.today()
                    if not default_re.search(param['description']):
                        param['description'] = u'{description} Default is {default}.'.format(**param)

                # Mark insignificant parameters
                if not membervalue.significant:
                    param['type'] = u'{type}, insignificant'.format(**param)

                # Append the param description and type
                lines.append(u':param {name}: {description}'.format(**param))
                lines.append(u':type {name}: {type}'.format(**param))

        # Append blank line to avoid warning
        lines.append(u'')

    return lines
