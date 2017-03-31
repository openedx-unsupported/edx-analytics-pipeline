..  _running_tasks:

Back to :doc:`index`

Tasks to Run to Update Insights
===============================

General Notes
-------------

#. These tasks are intended to be kicked off by some scheduler (Jenkins, cron etc)
#. You can use a script to automatically deploy a cluster on EMR, run the task and then shut it down. Here is an example: `run-automated-task.sh <https://github.com/edx/edx-analytics-configuration/blob/master/automation/run-automated-task.sh>`_.
#. Tweak ``NUM_REDUCE_TASKS`` based on the size of your cluster. If the cluster is not being used for anything else a good rule of thumb is to make ``NUM_REDUCE_TASKS`` equal the number of available reduce slots on your cluster.
#. There are a bunch of ``s3://`` paths listed below, but those could easily be replaced with ``hdfs://`` paths.
#. "credentials" files are json files should be stored somewhere secure and have the following format

  ::

    lms-creds.json

        {
          "host": "your.mysql.host.com",
          "port": "3306",
          "username": "someuser",
          "password": "passwordforsomeuser",
        }

#. I've started to put comments next to some parameters, please feel free to add in your own comments as well. Note that bash will get unhappy if you try to run these commands with those comments in place, be sure to strip them out of your commands before running. Also, ensure that there is no space following the trailing backslash on line endings.

Performance (Graded and Ungraded)
---------------------------------

Notes
~~~~~

* Intended to run nightly (or more frequently).
* This was one of the first tasks we wrote so it uses some deprecated patterns.
* You can tweak the event log pattern to restrict the amount of data this runs on, it will grab the most recent answer for each part of each problem for each student.
* You can find the source for building edx-analytics-hadoop-util.jar at `https://github.com/edx/edx-analytics-hadoop-util <https://github.com/edx/edx-analytics-hadoop-util>`_.

Task
~~~~

::

    AnswerDistributionWorkflow --local-scheduler \
      --src s3://path/to/tracking/logs/ \ [This should be the HDFS/S3 path to your tracking logs]
      --dest s3://folder/where/intermediate/files/go/ \ [This can be any location in HDFS/S3 that doesn't exist yet]
      --name unique_name \ [This can be any alphanumeric string, using the same string will attempt to use the same intermediate outputs etc]
      --output-root s3://final/output/path/ \ [This can be any location in HDFS/S3 that doesn't exist yet]
      --include '*tracking.log*.gz' \ [This glob pattern should match all of your tracking log files]
      --manifest "s3://scratch/path/to/manifest.txt" \ [This can be any path in HDFS/S3 that doesn't exist yet, a file will be written here]
      --base-input-format "org.edx.hadoop.input.ManifestTextInputFormat" \ [This is the name of the class within the jar to use to process the manifest]
      --lib-jar "hdfs://localhost:9000/edx-analytics-pipeline/packages/edx-analytics-hadoop-util.jar" \ [This is the path to the jar containing the above class, note that it should be an HDFS/S3 path]
      --n-reduce-tasks $NUM_REDUCE_TASKS \
      --marker $dest/marker \ [This should be an HDFS/S3 path that doesn't exist yet. If this marker exists, the job will think it has already run.]
      --credentials s3://secure/path/to/result_store_credentials.json [See discussion of credential files above, these should be the credentials for the result store database to write the result to]

Functional example:
~~~~~~~~~~~~~~~~~~~

::

    remote-task AnswerDistributionWorkflow --host localhost --user ubuntu --remote-name analyticstack --skip-setup --wait \
      --local-scheduler  --verbose \
      --src hdfs://localhost:9000/data \
      --dest hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/dest \
      --name pt_1449177792 \
      --output-root hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/course \
      --include "*tracking.log*.gz" \
      --manifest hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/manifest.txt \
      --base-input-format "org.edx.hadoop.input.ManifestTextInputFormat"  \
      --lib-jar hdfs://localhost:9000/edx-analytics-pipeline/site-packages/edx-analytics-hadoop-util.jar  \
      --n-reduce-tasks 1 \
      --marker hdfs://localhost:9000/tmp/pipeline-task-scheduler/AnswerDistributionWorkflow/1449177792/marker  \
      --credentials /edx/etc/edx-analytics-pipeline/output.json

The contents of a credentials file look something like this:

::

    {"username": "pipeline001", "host": "localhost", "password": "password", "port": 3306}


Enrollment
----------

Notes
~~~~~

* Intended to run nightly.
* This populates most of the data needed by the "Enrollment" lens in insights, including the demographic breakdowns by age, gender, and level of education.
* This uses more up-to-date patterns.
* Requires the following sections in config files: hive, database-export, database-import, map-reduce, event-logs, manifest, enrollments. The course-summary-enrollment and course-catalog-api sections are optional.
* It *does not* require the "enrollment-reports" section. That section is used to generate static CSV reports.
* The interval here, should be the beginning of time essentially. It computes enrollment by observing state changes from the beginning of time.

Task
~~~~

::

    ImportEnrollmentsIntoMysql --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

Incremental implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~

On September 29, 2016 we merged a modification of the Enrollment workflow to master.  The new code calculates Enrollment *incrementally*, rather than entirely from scratch each time.  And it involves a new parameter: ``overwrite_n_days``.

The workflow now assumes that new Hive-ready data has been written persistently to the ``course_enrollment_events`` directory under warehouse_path by CourseEnrollmentEventsTask.  The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days.  We are currently running with a value of 3, and we define that as an enrollment parameter in our override.cfg file.  You can define it there or on the command line.

This means for us that only the last three days of raw events get scanned nightly.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.

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

* Intended to run nightly.
* This populates the map view in insights.
* This is also one of our older tasks.
* Finds the most recent event for every user and geolocates the IP address on the event.
* This currently uses the student_courseenrollment table to figure out which users are enrolled in which courses. It should really be using the "course_enrollment" table computed by the enrollment and demographics related tasks.
* This no longer supports a separate ``user-country-output`` parameter for intermediate data.  This is now written to a dated partition under ``(warehouse_path)/last_country_of_user/``.
* Requires a maxmind data file (country granularity) to be uploaded to HDFS or S3 (see the ``geolocation`` section of the config file).  Getting a data file could look like this:

::

      wget http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz
      gunzip GeoIP.dat.gz
      mv GeoIP.dat geo.dat
      hdfs dfs -put geo.dat /edx-analytics-pipeline/


Task
~~~~

::

    InsertToMysqlCourseEnrollByCountryWorkflow --local-scheduler \
     --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
     --course-country-output $INTERMEDIATE_OUTPUT_ROOT/$(date +%Y-%m-%d -d "$TO_DATE")/country_course \
     --n-reduce-tasks $NUM_REDUCE_TASKS \
     --overwrite

Incremental implementation
~~~~~~~~~~~~~~~~~~~~~~~~~~

On November 19, 2016 we merged a modification of the Location workflow to master.  The new code calculates Location *incrementally*, rather than entirely from scratch each time.  And it involves a new parameter: ``overwrite_n_days``.

The workflow now assumes that new Hive-ready data has been written persistently to the ``last_ip_of_user`` directory under warehouse_path by LastDailyIpAddressOfUserTask.  The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days.  We are currently running with a value of 3, and we define that as an enrollment parameter in our override.cfg file.  You can define it there (as ``overwrite_n_days`` in the ``[location-per-course]`` section) or on the command line (as ``--overwrite-n-days``).

This means for us that only the last three days of raw events get scanned nightly.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.

Another change is to allow the interval start to be defined in configuration (as ``interval_start`` in the ``[location-per-course]`` section).  One can then specify instead just the end date on the workflow:

::

    InsertToMysqlCourseEnrollByCountryWorkflow --local-scheduler \
     --interval-end $(date +%Y-%m-%d -d "$TO_DATE") \
     --course-country-output $INTERMEDIATE_OUTPUT_ROOT/$(date +%Y-%m-%d -d "$TO_DATE")/country_course \
     --n-reduce-tasks $NUM_REDUCE_TASKS \
     --overwrite

On December 5, 2016 the ``--course-country-output`` parameter was removed.  That data is instead written to the warehouse_path.

History task
~~~~~~~~~~~~

To load the historical enrollment events, you would need to first run:

::

    LastDailyIpAddressOfUserTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

Note that this does not use the ``interval_start`` configuration value, so specify the full interval.

Engagement
----------

Notes
~~~~~

* Intended to be run weekly

Task
~~~~

::

    CourseActivityWeeklyTask --local-scheduler \
      --end-date $(date +%Y-%m-%d -d "$TO_DATE") \
      --weeks 24 \
      --credentials $CREDENTIALS \
      --n-reduce-tasks $NUM_REDUCE_TASKS

Video
~~~~~

Notes
~~~~~

* Intended to be run daily.
* Still a work in progress - erroneous events can make videos appear to be much longer than they actually are.

Task
~~~~

::

    InsertToMysqlAllVideoTask --local-scheduler \
      --interval $(date +%Y-%m-%d -d "$FROM_DATE")-$(date +%Y-%m-%d -d "$TO_DATE") \
      --n-reduce-tasks $NUM_REDUCE_TASKS

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

The workflow assumes that new Hive-ready data has been written persistently to the ``module_engagement`` directory under warehouse_path by ModuleEngagementIntervalTask.  The workflow uses the ``overwrite_n_days`` to determine how many days back to repopulate this data. The idea is that before this point, events are not expected to change, but perhaps there might be new events that have arrived in the last few days.  We are currently running with a value of 3, and this can be overridden on the command-line or defined as a ``[module-engagement]`` parameter in the override.cfg file.  This means for us that only the last three days of raw events get scanned nightly.  It is assumed that the previous days' data has been loaded by previous runs, or by performing a historical load.

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

We run the module engagement job nightly, which adds the most recent day to this while it is overwriting the last N days (as set by the ``--overwrite-n-days`` parameter).  This calculates aggregates and loads them into ES and Mysql.

::

    ModuleEngagementWorkflowTask --local-scheduler \
      --date $(date +%Y-%m-%d -d "$TO_DATE") \
      --indexing-tasks 5 \
      --throttle 0.5 \
      --n-reduce-tasks $NUM_REDUCE_TASKS

The value of ``TO_DATE`` is today.
