.. edX analytics pipeline documentation master file, created by
   sphinx-quickstart on Thu Feb 18 11:55:18 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to edX analytics pipeline's documentation!
==================================================

Contents:

.. toctree::
   :maxdepth: 2


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Analytics tasks
===============

.. this directive sets the module for all other directives until next `module` is seen.
.. module:: edx.analytics.tasks.answer_dist


.. autoclass directive will put AnswerDistributionWorkflow docs by extracting class docstring.
   `autoclass` (as it's "manual" counterpart `class`) honors current module (set by `module` directive), so
   it will search AnswerDistributionWorkflow in edx.analytics.tasks.answer_dist module.
   * members directive sets the list class of members to include into documentation. In this case, only
     `answer_metadata`, `src`, `dest`, `name`, `output_root` and `include` will be documented.
   *  undoc-members will allow members with no docstrings to appear in the generated docs (by default they are excluded)

   See http://www.sphinx-doc.org/en/stable/ext/autodoc.html?highlight=autodoc#directive-autoclass for more details

.. autoclass:: AnswerDistributionWorkflow
   :members: answer_metadata, src, dest, name, output_root, include
   :undoc-members:

.. Switches to edx.analytics.tasks.enrollments module
.. module:: edx.analytics.tasks.enrollments

.. Same idea as before, but this time `members` attribute is empty. In this case, Sphinx puts all the public methods,
   properties and attributes, excpet those inherited from parent classes. So, for ImportEnrollmentsIntoMysql it will
   only show `requires` method

.. autoclass:: ImportEnrollmentsIntoMysql
   :members:
   :undoc-members:

.. Alternatively, module directive can be omitted and full-qualified class name can be used
   Another concept demonstrated here is the use of `inherited-members`. With this flag set, Sphinx outputs *all* the
   class members, including inherited, "magic" and undocumented ones.

.. autoclass:: edx.analytics.tasks.event_type_dist.PushToVerticaEventTypeDistributionTask
   :inherited-members:

