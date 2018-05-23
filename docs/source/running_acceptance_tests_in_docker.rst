..  _running_acceptance_tests_in_docker:

Running Acceptance Tests in Docker Analyticstack
================================================

For docker analyticstack setup, follow instructions under **Getting Started on Analytics** section in `Devstack`_ repository.

Pre-requisites
--------------

* Access analytics pipeline shell:

   ::

       make analytics-pipeline-shell

* Before running the user-location workflows, a geolocation Maxmind data file must be downloaded. This file can be in
  HDFS or S3, for example, and should be pointed to by the `geolocation_data` setting in the `geolocation` section of your
  configuration file. To use the default location used by acceptance tests, execute the following:

   ::

       curl -fSL http://geolite.maxmind.com/download/geoip/database/GeoLiteCountry/GeoIP.dat.gz -o /var/tmp/GeoIP.dat.gz
       cd /var/tmp/ && gunzip /var/tmp/GeoIP.dat.gz
       mv GeoIP.dat geo.dat
       hdfs dfs -put geo.dat /edx-analytics-pipeline/

Running Acceptance Tests
------------------------

* To run the full test suite, execute the following command in the shell:

  ::

      make docker-test-acceptance-local-all

* To run individual tests, execute the following:

  ::

      make docker-test-acceptance-local ONLY_TESTS=edx.analytics.tasks.tests.acceptance.<test_script_file>    # e.g.
      make docker-test-acceptance-local ONLY_TESTS=edx.analytics.tasks.tests.acceptance.test_user_activity


.. _Devstack: https://github.com/edx/devstack/tree/master/README.rst#getting-started-on-analytics
