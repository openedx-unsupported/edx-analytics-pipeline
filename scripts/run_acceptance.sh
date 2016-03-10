#!/usr/bin/env bash
#
# Use this script to run the entire suite of acceptance tests
# or one or more specific test modules/classes/methods
#

function usage
{
    echo "Usage: ./run_acceptance.sh [[[-t test] [-u]] | [-h]]"
}

TESTS=()
UPDATE_VENV=

while [ "$1" != "" ]; do
    case "$1" in
        -t | --test )
            shift
            TEST="$1"
            TESTS+=("edx.analytics.tasks.tests.acceptance."$TEST)
            ;;
        -u | --update )
            UPDATE_VENV=1
            ;;
        -h | --help )
            usage
            exit
            ;;
        * )
            usage
            exit 1
    esac
    shift
done

# Environment

WORKING_DIRECTORY=/var/lib/analytics-tasks/devstack/repo

TASKS_REPO=/edx/app/analytics_pipeline/analytics_pipeline

# Check if repo has been cloned and mounted at default location
if [ ! -d "$TASKS_REPO" ]
then
    echo "Please make sure edx-analytics-pipeline is mounted at ${TASKS_REPO}. See README.md for more info."
    exit 1
fi

# Prepare environment as "vagrant" user
if [ -n "$UPDATE_VENV" ]
then
    sudo -Hu vagrant /bin/bash<<-PREPARE
cd $WORKING_DIRECTORY

# Activate virtualenv
source ../venv/bin/activate

# Prepare virtualenv for testing
make develop-local
make test-requirements
PREPARE
fi

# Tests

# Run tests as "hadoop" user
sudo -Hu hadoop /bin/bash<<-RUN
cd $WORKING_DIRECTORY

# Activate virtualenv
source ../venv/bin/activate

# Run tests
make test-acceptance-local ONLY_TESTS="${TESTS[@]}"
RUN
