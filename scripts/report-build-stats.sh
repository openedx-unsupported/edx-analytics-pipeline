#!/usr/bin/env bash
#
# Using test-metrics, this script will post
# stats to datadog.
#
#

if [ -n "${DATADOG_API_KEY}" ];

    then
        echo "Reporting coverage stats to datadog"

        rm -rf test-metrics
        git clone https://github.com/wedaly/test-metrics

        cd test-metrics
        pip install -q -r requirements.txt

        cat > unit_test_groups.json <<END
{
    "unit.analytics_pipeline": "edx/*.py"
}
END
        python -m metrics.coverage unit_test_groups.json ../coverage.xml

    else
        echo "Skipping sending stats to datadog. DATADOG_API_KEY not set."

fi

