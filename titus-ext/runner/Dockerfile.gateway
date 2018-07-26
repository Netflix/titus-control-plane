FROM openjdk:8 as builder
ENV GRADLE_OPTS="-Dorg.gradle.daemon=false"
## install gradle
COPY gradlew gradle.properties /usr/src/titus-control-plane/
COPY gradle /usr/src/titus-control-plane/gradle/
WORKDIR /usr/src/titus-control-plane
# force gradle to download a distribution
RUN ./gradlew tasks
## build titus-server-gateway
COPY . /usr/src/titus-control-plane
RUN ./gradlew -PdisablePrivateRepo=true :titus-server-gateway:installDist


FROM openjdk:8-jre
COPY --from=builder /usr/src/titus-control-plane/titus-server-gateway/build/install/titus-server-gateway \
     /opt/titus-server-gateway
COPY --from=builder /usr/src/titus-control-plane/titus-ext/runner/titusgateway.properties \
     /opt/titus-server-gateway/etc/titusgateway.properties
WORKDIR /opt/titus-server-gateway
EXPOSE 7001/tcp 7101/tcp 7104/tcp
ENV JAVA_OPTS="-XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap"
CMD ["./bin/titus-server-gateway", "-p", "./etc/titusgateway.properties"]
