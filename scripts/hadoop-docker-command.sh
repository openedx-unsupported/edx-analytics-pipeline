#!/bin/bash

apt-get -y update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python python-setuptools

exec /run.sh