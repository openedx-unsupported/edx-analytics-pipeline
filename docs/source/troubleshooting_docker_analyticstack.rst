..  _troubleshooting_docker_analyticstack:

Troubleshooting Docker Analyticstack
====================================

* Make sure there are no errors during provision command. If there are errors, **do not rerun the provision command without first cleaning up after the failures.**
* For cleanup, there are 2 options.

  - Reset containers ( this will remove all containers and volumes )

     ::

         make destroy

  - Manual cleanup

     ::

         make mysql-shell
         mysql
         DROP DATABASE reports
         DROP DATABASE edx_hive_metastore
         DROP DATABASE edxapp           # Only drop if provisioning failed while loading the LMS schema.
         DROP DATABASE edxapp_csmh      # Only drop if provisioning failed while loading the LMS schema.
         # exit mysql shell
         make down
