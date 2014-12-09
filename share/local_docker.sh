#!/bin/bash

sudo DOCKER_CONFIG_FILE=./docker.yml $VIRTUAL_ENV/bin/python docker.py --list
