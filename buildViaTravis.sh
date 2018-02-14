#!/bin/bash
# This script will build the project.

if [ "$TITUS_INTEGRATION_TEST" == "true" ]; then
  echo -e "Running integration tests (PR $TRAVIS_PULL_REQUEST) => Branch [$TRAVIS_BRANCH]"
  ./gradlew -PtitusUsername="${titusUsername}" -PtitusPassword="${titusPassword}" integrationTest --stacktrace
elif [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
  echo -e "Build Pull Request #$TRAVIS_PULL_REQUEST => Branch [$TRAVIS_BRANCH]"
  ./gradlew -PtitusUsername="${titusUsername}" -PtitusPassword="${titusPassword}" build --stacktrace
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" == "" ]; then
  echo -e 'Build Branch with Snapshot => Branch ['$TRAVIS_BRANCH']'
  ./gradlew -PtitusUsername="${titusUsername}" -PtitusPassword="${titusPassword}" -Prelease.travisci=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" build --stacktrace
elif [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_TAG" != "" ]; then
  echo -e 'Build Branch for Release => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']'
  case "$TRAVIS_TAG" in
  *-rc\.*)
    ./gradlew -PtitusUsername="${titusUsername}" -PtitusPassword="${titusPassword}" -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" candidate --stacktrace
    ;;
  *)
    ./gradlew -PtitusUsername="${titusUsername}" -PtitusPassword="${titusPassword}" -Prelease.travisci=true -Prelease.useLastTag=true -PbintrayUser="${bintrayUser}" -PbintrayKey="${bintrayKey}" final --stacktrace
    ;;
  esac
else
  echo -e 'WARN: Should not be here => Branch ['$TRAVIS_BRANCH']  Tag ['$TRAVIS_TAG']  Pull Request ['$TRAVIS_PULL_REQUEST']'
  ./gradlew -PtitusUsername="${titusUsername}" -PtitusPassword="${titusPassword}" build --stacktrace
fi
