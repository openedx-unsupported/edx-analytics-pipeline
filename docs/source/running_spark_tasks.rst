..  _running_spark_tasks:

Running Spark Tasks in Docker Analyticstack
===========================================

Pre-requisites
--------------

* Access analytics pipeline shell:

   ::

       make analytics-pipeline-shell

* Generate egg files

  If you plan to run Spark workflows that use imports that in turn require the use of a plugin mechanism,
  it is necessary to store those imports locally as egg files. These imports are then identified in the
  configuration file in the `spark` section. Opaque keys is one of these imports, and the two egg files
  used by Spark can be made as follows.

   ::

       make generate-spark-egg-files

Task
~~~~

::

    launch-task UserActivityTaskSpark --local-scheduler --interval 2017-03-16
