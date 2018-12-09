
..  _faq:

Back to :doc:`index`
    
Analyticstack Frequently Asked Questions
=========================================

This page is intended to provide answers to particular questions that
may arise while using analyticstack for development.  Some topics may
also be relevant to those working outside of analyticstack as well.

* :ref:`Adding events to a tracking.log for testing`
* :ref:`Avoiding Java out-of-memory errors`
* :ref:`Running acceptance tests that include my changes`
* :ref:`Getting tasks to re-run`
* :ref:`Accessing more detailed logging`
* :ref:`Inspecting Hive tables populated by acceptance tests`

.. _Adding events to a tracking.log for testing:  

Adding events to a tracking.log for testing
###########################################

To test a new pipeline feature, I need new events in the tracking.log file.

Solution
--------

Don't try to edit the tracking.log in HDFS directly as it is frequently overwritten by a cron job. Instead:

 #. Create a new file titled something like "custom-tracking.log" (filename must end in "tracking.log")

 #. Add the events that you need to the file, one event per line.

    * Make sure that any course_id field has the value of
      "edX/DemoX/Demo_Course" and org_id = "edX"
      
    * To view example events in the existing tracking.log, run (as the
      hadoop user in the analyticstack)::

	hadoop fs -cat /data/tracking.log

 #. Upload the file to HDFS. Run (as the hadoop user in the
    analyticstack)::
      
      hadoop fs -put custom-tracking.log /data/custom-tracking.log
    
 #. Now you can run the task you are testing.
    The output should print that it is sourcing events from 2 files
    now.
    
 #. If you need to modify the events you added, edit the
    "custom-tracking.log" on the normal file system and then run the
    following::

      hadoop fs -rm /data/custom-tracking.log
      hadoop fs -put custom-tracking.log /data/custom-tracking.log

.. _Avoiding Java out-of-memory errors:

Avoiding Java out-of-memory errors
##################################

I keep getting Java out-of-memory errors, aka. 143 error code, when I run tasks.

Solution
--------

Something is likely misconfigured, and the JVM is not allocating enough memory.

#. First, make sure the virtual machine has enough virtual memory
   configured. Open the VirtualBox GUI and check that the machine
   whose title starts with "analyticstack" has around 4GB of memory
   assigned to it.
   
#. If one is getting an error about virtual memory exceeding a limit,
   then turn off vmem-check in yarn. As the vagrant user in the analytics
   stack, edit the yarn-site.xml config to add a property::

     sudo nano /edx/app/hadoop/hadoop/etc/hadoop/yarn-site.xml

   Inside the <configuration> add the property:
   
   .. code:: xml
	       
      <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
      </property>
 
   Then run the following to restart yarn so that the config change is
   registered::
     
     sudo service yarn restart


.. _Running acceptance tests that include my changes:

Running acceptance tests that include my changes
################################################

I made local changes to acceptance tests, but tests don't seem to be running with the changes.

Solution
--------

Acceptance tests are checked out of your branch.  Push your changes to
your branch and to rerun the acceptance tests.  Note that a `git
commit` is the only thing required, since it's pulling from your local
branch, not the remote.

 * Commit your changes to your branch.
 * Rerun the acceptance tests.


.. _Getting tasks to re-run:

Getting tasks to re-run
#######################

I re-ran a task, but the output didn't change.

Solution
--------

One possible reason for this issue is that the task is not actually being re-run.

The task scheduler will skip running tasks if it recognizes that it
has been run before and it can use the existing output instead of
re-running it. At the beginning of the output of the command, each
task is scheduled. If the line ends with "(DONE)" then the scheduler
has recognized that it was run before and will not rerun it. If it is
marked as "(PENDING)" then it is actually scheduled to run. 

There are a few ways of tricking the scheduler into re-running tasks:

 * Pass different parameters to the task command on the
   command-line. As long as the task has not been run with those
   parameters before, it may force it to re-run tasks because the
   source data is different.
   
 * Remove the output of the task.  The task scheduler (luigi) runs the
   "complete" function on each task to determine whether a task has
   been run before. This can be different for every task, but
   typically it checks the output table of the command for any
   data. Deleting the output table can cause the complete function to
   return false and force a re-run.
    
   * If the output is a hive table, then, as the hadoop user in the
     analyticstack, run::
       
       hive -e "drop table <table_name>;"

   * If the output is a mysql table, then, as the vagrant user in the
     analyticstack, run::
       
       sudo mysql --database=<database_name> -e "drop table <table_name>;"
       
   * If the output are files in the "warehouse" location in HDFS,
     then, as the hadoop user in analyticstack, run::
       
       hadoop fs -rm -R /edx-analytics-pipeline/warehouse/<tablename>



.. _Accessing more detailed logging:

Accessing more detailed logging
###############################

I need to see more detailed logs than what is sent to standard-out.

Solution
--------

In the analyticstack,
/edx/app/analytics_pipeline/analytics_pipeline/edx_analytics.log
includes what goes to standard-out plus DEBUG level logging.

To see detailed hadoop logs, find and copy the application_id printed
in the output of a task run and pass it to this command::
  
  yarn logs -applicationId <application_id>


.. _Inspecting Hive tables populated by acceptance tests:

Inspecting Hive tables populated by acceptance tests
####################################################

My acceptance tests are failing, and I want to look at the hive tables.

Solution
--------

Query the acceptance test DB via hive.

    * Within the analytics devstack, switch to the hadoop user::
	
	sudo hadoop

    * Start up hive::
	
	hive

    * Find the acceptance test database::
	
        show databases;

    * Show tables for your database::
	
	use test_283482342;  # your database will be different
	show tables;

    * Execute your queries.

