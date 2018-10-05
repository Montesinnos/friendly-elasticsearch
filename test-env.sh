#!/usr/bin/env bash
#Starts a new docker environment containing a test cluster to run integration tests

echo "Removing any existing test environment"
docker-compose -f ./docker/docker-compose-test.yml down  --remove-orphans
echo "Building new test environment"
docker-compose -f ./docker/docker-compose-test.yml build
docker-compose -f ./docker/docker-compose-test.yml up
echo "Test environment is ready"