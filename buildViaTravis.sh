#!/bin/bash
# This script will build the project.

if [ "$TITUS_DOCKER_COMPOSE" == "true" ]; then
  echo -e "Running docker-compose (PR $TRAVIS_PULL_REQUEST) => Branch [$TRAVIS_BRANCH]"
  set -eux
  docker-compose build
  docker-compose -d up
  curl --fail \
    --max-time 10 --connect-timeout 5 \
    --retry 100 --retry-max-time 600 --retry-connrefused \
    localhost:7001/api/v2/status

elif [ "$TITUS_INTEGRATION_TEST" == "true" ]; then
  echo -e "Running integration tests (PR $TRAVIS_PULL_REQUEST) => Branch [$TRAVIS_BRANCH]"
  ./gradlew integrationTest --stacktrace

elif [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
  echo -e "Build Pull Request #$TRAVIS_PULL_REQUEST => Branch [$TRAVIS_BRANCH]"
  ./gradlew build --stacktrace

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ]; then
  echo -e 'Build Branch with Snapshot => Branch ['$TRAVIS_BRANCH']'
  ./gradlew -Prelease.travisci=true build --stacktrace

elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ]; then
  echo -e 'Build Branch for Release => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']'
  case "$TRAVIS_TAG" in
  *-rc\.*)
    ./gradlew -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" candidate --stacktrace
    ;;
  *)
    ./gradlew -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" -PsonatypeUsername="${sonatypeUsername}" -PsonatypePassword="${sonatypePassword}" final --stacktrace
    ;;
  esac

else
  echo -e 'WARN: Should not be here => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']  Pull Request ['$TRAVIS_PULL_REQUEST']'
  ./gradlew build --stacktrace
fi
