..  _running_tasks:

Back to :doc:`index`

Tasks to Run to Update Insights
===============================

General Notes
-------------

#. These tasks are intended to be kicked off by some scheduler (Jenkins, cron etc)
#. You can use a script to automatically deploy a cluster on EMR, run the task and then shut it down. Here is an example: `run-automated-task.sh <https://github.com/edx/edx-analytics-configuration/blob/master/automation/run-automated-task.sh>`_.
#. Tweak ``NUM_REDUCE_TASKS`` based on the size of your cluster. If the cluster is not being used for anything else a good rule of thumb is to make ``NUM_REDUCE_TASKS`` equal the number of available reduce slots on your cluster. See hadoop docs to determine the number of reduce slots available on your cluster.
#. Luigi, the underlying workflow engine, has support for both S3 and HDFS when specifying input and output paths. ``s3://`` can be replaced with ``hdfs://`` in all examples below.
#. "credentials" files are json files should be stored somewhere secure and have the following format. They are often stored in S3 or HDFS but can also be stored on the local filesystem of the machine running the data pipeline.

  ::

    lms-creds.json

        {
          "host": "your.mysql.host.com",
          "port": "3306",
          "username": "someuser",
          "password": "passwordforsomeuser",
        }


Performance (Graded and Ungraded)
---------------------------------

Notes
~~~~~

* Intended to run daily (or more frequently).
* This was one of the first tasks we wrote so it uses some deprecated patterns.
* You can tweak the event log pattern to restrict the amount of data this runs on, it will grab the most recent answer for each part of each problem for each student.
* You can find the source for building edx-analytics-hadoop-util.jar at `https://github.com/edx/edx-analytics-hadoop-util <https://github.com/edx/edx-analytics-hadoop-util>`_.

Task
~~~~

::

    AnswerDistributionWorkflow --local-scheduler \
      --src ["s3://path/to/tracking/logs/"] \
      --dest s3://folder/where/intermediate/files/go/ \
      --name unique_name \
      --output-root s3://final/output/path/ \
      --include ["*tracking.log*.gz"] \
      --manifest "s3://scratch/path/to/manifest.txt" \
      --base-input-format "org.edx.hadoop.input.ManifestTextInputFormat" \
      --lib-jar ["hdfs://localhost:9000/edx-analytics-pipeline/packages/edx-analytics-hadoop-util.jar"] \
      --n-reduce-tasks $NUM_REDUCE_TASKS \
      --marker $dest/marker \
      --credentials s3://secure/path/to/result_store_credentials.json

Parameter Descriptions
~~~~~~~~~~~~~~~~~~~~~~

* ``--src``: This should be a list of HDFS/S3 paths to the root (or roots) of your tracking logs, expressed as a JSON list.
* ``--dest``: This can be any location in HDFS/S3 that doesn't exist yet.
* ``--name``: This can be any alphanumeric string, using the same string will attempt to use the same intermediate outputs etc.
* ``--output-root``: This can be any location in HDFS/S3 that doesn't exist yet.
* ``--include``: This glob pattern should match all of your tracking log files, and be expressed as a JSON list.
* ``--manifest``: This can be any path in HDFS/S3 that doesn't exist yet, a file will be written here.
* ``--base-input-format``: This is the name of the class within the jar to use to process the manifest.
* ``--lib-jar``: This is the path to the jar containing the above class.  Note that it should be an HDFS/S3 path, and expressed as a JSON list.
* ``--n-reduce-tasks``: Number of reduce tasks to schedule.
* ``--marker``: This should be an HDFS/S3 path that doesn't exist yet. If this marker exists, the job will think it has already run.
* ``--credentials``: See discussion of credential files above. These should be the credentials for the result store database to write the result to.

Functional example:
~~~~~~~~~~~~~~~~~~~

::

    remote-task AnswerDistributionWorkflow --host localhost --user ubuntu --remote-name analyticstack --skip-setup --wait \
      --local-scheduler  --verbose \
      --src ["hdfs://localhost:9000/data"] \
      --dest hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/dest \
      --name pt_1449177792 \
      --output-root hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/course \
      --include ["*tracking.log*.gz"] \
      --manifest hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/manifest.txt \
      --base-input-format "org.edx.hadoop.input.ManifestTextInputFormat"  \
      --lib-jar ["hdfs://localhost:9000/edx-analytics-pipeline/site-packages/edx-analytics-hadoop-util.jar"]  \
      --n-reduce-tasks 1 \
      --marker hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/marker  \
      --credentials /edx/etc/edx-analytics-pipeline/output.json

Enrollment
----------

Notes
~~~~~

* Intended to run daily.
* This populates most of the data needed by the "Enrollment" lens in insights, including the demographic breakdowns by age, gender, and level of education.
* Requires the following sections in config files: hive, database-export, database-import, map-reduce, event-logs, manifest, enrollments. The course-summary-enrollment and course-catalog-api sections are optional.
* The interval here, should be the beginning of time essentially. It computes enrollment by observing state changes from the beginning of time.
* ``$FROM_DATE`` can be any string that is accepted by the unix utility ``date``. Here are a few examples: "today", "yesterday", and "2016-05-01".
* ``overwrite_mysql`` controls whether or not the MySQL tables are replaced in a transaction during processing.  Set this flag if you are fully replacing the table, false (default) otherwise.
* ``overwrite_hive`` controls whether or not the Hive intermediate table metadata is removed and replaced during processing.  Set this flag if you want the metadata to be fully recreated, false (default) otherwise.

Task
~~~~

::

    ImportEnrollmentsIntoMysql --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS \
      --overwrite-mysql \
      --overwrite-hive

Incremental implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~

On September 29, 2016 we merged a modification of the Enrollment workflow to master.  The new code calculates Enrollment *incrementally*, rather than entirely from scratch each time.  And it involves a new parameter: ``overwrite_n_days``.

The workflow now assumes that new Hive-ready data has been written persistently to the ``course_enrollment_events`` directory under warehouse_path by CourseEnrollmentEventsTask.  The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days.  We are currently running with a value of 3, and we define that as an enrollment parameter in our override.cfg file.  You can define it there or on the command line.

This means for us that only the last three days of raw events get scanned daily.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.

History task
~~~~~~~~~~~~

To load the historical enrollment events, you would need to first run:

::

    CourseEnrollmentEventsTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

Geography
---------

Notes
~~~~~

* Intended to run daily.
* This populates the map view in insights.
* This is also one of our older tasks.
* Finds the most recent event for every user and geolocates the IP address on the event.
* This currently uses the student_courseenrollment table to figure out which users are enrolled in which courses. It should really be using the "course_enrollment" table computed by the enrollment and demographics related tasks.
* Requires a maxmind data file (country granularity) to be uploaded to HDFS or S3 (see the ``geolocation`` section of the config file).  Getting a data file could look like this:

::

      wget http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz
      gunzip GeoIP.dat.gz
      mv GeoIP.dat geo.dat
      hdfs dfs -put geo.dat /edx-analytics-pipeline/


Task
~~~~

::

    InsertToMysqlLastCountryPerCourseTask --local-scheduler \
     --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
     --course-country-output $INTERMEDIATE_OUTPUT_ROOT/$(date +%Y-%m-%d -d "$TO_DATE")/country_course \
     --n-reduce-tasks $NUM_REDUCE_TASKS \
     --overwrite

Incremental implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~

On November 19, 2016 we merged a modification of the Location workflow to master.  The new code calculates Location *incrementally*, rather than entirely from scratch each time.  And it involves a new parameter: ``overwrite_n_days``.

The workflow now assumes that new Hive-ready data has been written persistently to the ``last_ip_of_user_id`` directory under warehouse_path by LastDailyIpAddressOfUserTask.(Before May 9,2018, this used the ``last_ip_of_user`` directory for output.) The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days.  We are currently running with a value of 3, and we define that as an enrollment parameter in our override.cfg file.  You can define it there (as ``overwrite_n_days`` in the ``[location-per-course]`` section) or on the command line (as ``--overwrite-n-days``).

This means for us that only the last three days of raw events get scanned daily.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.

Another change is to allow the interval start to be defined in configuration (as ``interval_start`` in the ``[location-per-course]`` section).  One can then specify instead just the end date on the workflow:

::

    InsertToMysqlLastCountryPerCourseTask --local-scheduler \
     --interval-end $(date +%Y-%m-%d -d "$TO_DATE") \
     --course-country-output $INTERMEDIATE_OUTPUT_ROOT/$(date +%Y-%m-%d -d "$TO_DATE")/country_course \
     --n-reduce-tasks $NUM_REDUCE_TASKS \
     --overwrite

On December 5, 2016 the ``--course-country-output`` parameter was removed.  That data is instead written to the warehouse_path.

History task
~~~~~~~~~~~~

To load the historical location data, you would need to first run:

::

    LastDailyIpAddressOfUserTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

Note that this does not use the ``interval_start`` configuration value, so specify the full interval.

Engagement
----------

Notes
~~~~~

* Intended to be run weekly or daily.
* When using a persistent hive metastore, set ``overwrite_hive`` to True.

Task
~~~~

::

    InsertToMysqlCourseActivityTask --local-scheduler \
      --end-date $(date +%Y-%m-%d -d "$TO_DATE") \
      --weeks 24 \
      --credentials $CREDENTIALS \
      --n-reduce-tasks $NUM_REDUCE_TASKS \
      --overwrite-mysql

Incremental implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~

On December 05, 2017 we merged a modification of the Engagement workflow to master.  The new code calculates Engagement *incrementally*, rather than entirely from scratch each time.  And it involves a new parameter: ``overwrite_n_days``.

Also, the workflow has been renamed from ``CourseActivityWeeklyTask`` to ``InsertToMysqlCourseActivityTask``.

The workflow now assumes that new Hive-ready data has been written persistently to the ``user_activity`` directory under warehouse_path by UserActivityTask.  The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days.  We are currently running the workflow daily with a value of 3, and we define that as an user-activity parameter in our override.cfg file.  You can define it there or on the command line.

This means for us that only the last three days of raw events get scanned daily.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.

If this workflow is run weekly, an ``overwrite_n_days`` value of 10 would be more appropriate.

History task
~~~~~~~~~~~~

To load the historical user-activity counts, you would need to first run:

::

    UserActivityTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

or you could run the incremental workflow with an ``overwrite_n_days`` value large enough that it would
calculate the historical user-activity counts the first time it is ran:


::

    InsertToMysqlCourseActivityTask --local-scheduler \
      --end-date $(date +%Y-%m-%d -d "$TO_DATE") \
      --weeks 24 \
      --credentials $CREDENTIALS \
      --n-reduce-tasks $NUM_REDUCE_TASKS \
      --overwrite-n-days 169

After the first run, you can change ``overwrite_n_days`` to 3 or 10 depending on how you plan to run it(daily/weekly).

Video
~~~~~

Notes
~~~~~

* Intended to be run daily.

Task
~~~~

::

    InsertToMysqlAllVideoTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

Incremental implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~

On October 16, 2017 we merged a modification of the Video workflow to master.  The new code calculates Video *incrementally*, rather than entirely from scratch each time.  And it involves a new parameter: ``overwrite_n_days``.


The workflow now assumes that new Hive-ready data has been written persistently to the ``user_video_viewing_by_date`` directory under warehouse_path by UserVideoViewingByDateTask.  The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days, particularly if coming from a mobile source.  We are currently running the workflow daily with a value of 3, and we define that as a video parameter in our override.cfg file.  You can define it there or on the command line.

This means for us that only the last three days of raw events get scanned daily.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.


History task
~~~~~~~~~~~~

To load the historical video counts, you would need to first run:

::

    UserVideoViewingByDateTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

or you could run the incremental workflow with an ``overwrite_n_days`` value large enough that it would
calculate the historical video counts the first time it is ran:


::

    InsertToMysqlAllVideoTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS
      --overwrite-n-days 169

After the first run, you can change ``overwrite_n_days`` to 3.


Learner Analytics
-----------------

Notes
~~~~~

* Intended to run daily.
* This populates most of the data needed by the "Learner Analytics" lens in insights.
* This uses more up-to-date patterns.
* Requires the following sections in config files: hive, database-export, database-import, map-reduce, event-logs, manifest, module-engagement.
* It is an incremental implementation, so it requires persistent storage of previous runs.  It also requires an initial load of historical data.
* Requires the availability of a separate ElasticSearch instance running 1.5.2.  This is different from the version that the LMS uses, which is still on 0.90.

History task
~~~~~~~~~~~~

The workflow assumes that new Hive-ready data has been written persistently to the ``module_engagement`` directory under warehouse_path by ModuleEngagementIntervalTask.  The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days.  We are currently running with a value of 3, and this can be overridden on the command-line or defined as a ``[module-engagement]`` parameter in the override.cfg file.  This means for us that only the last three days of raw events get scanned daily.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.

To load module engagement history, you would first need to run:

::

    ModuleEngagementIntervalTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS \
      --overwrite-from-date $(date +%Y-%m-%d -d "$TO_DATE") \
      --overwrite-mysql

Since module engagement in Insights only looks at the last two weeks of activity, you only need ``FROM_DATE`` to be two weeks ago.  The ``TO_DATE`` need only be within N days of today (as specified by ``--overwrite-n-days``).  Setting ``--overwrite-mysql`` will ensure that all the historical data is also written to the Mysql Result Store.  Using ``--overwrite-from-date`` is important when "fixing" data (for some reason): setting it earlier (i.e. to ``FROM_DATE``) will cause the Hive data to also be overwritten for those earlier days.

Another prerequisite before running the module engagement workflow below is to have run enrollment first.  It is assumed that the ``course_enrollment`` directory under warehouse_path has been populated by running enrollment with a ``TO_DATE`` matching that used for the module engagement workflow (i.e. today).

Task
~~~~

We run the module engagement job daily, which adds the most recent day to this while it is overwriting the last N days (as set by the ``--overwrite-n-days`` parameter).  This calculates aggregates and loads them into ES and MySQL.

::

    ModuleEngagementWorkflowTask --local-scheduler \
      --date $(date +%Y-%m-%d -d "$TO_DATE") \
      --indexing-tasks 5 \
      --throttle 0.5 \
      --n-reduce-tasks $NUM_REDUCE_TASKS

The value of ``TO_DATE`` is today.
