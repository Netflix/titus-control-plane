#!/usr/bin/env bash
#
# Build sources by running:
# mvn clean package source:jar

K_CLIENT_HOME=${K_CLIENT_HOME:-"${HOME}/projects/opensource/kubernetes-java-client"}

find ${K_CLIENT_HOME} -name "client-java*jar"|egrep -v -e "(with-dependencies|examples|javadoc|tests)" | while read jar; do
  b_name=$(basename ${jar})
  t_name=$(echo ${b_name} | sed -e "s/beta1-SNAPSHOT/netflixbeta/")
  echo "${b_name} -> ${t_name}"
  mv -f ${jar} ${t_name}
done
