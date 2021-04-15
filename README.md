# Titus Control Plane

[![Build Status](https://travis-ci.org/Netflix/titus-control-plane.svg?branch=master)](https://travis-ci.org/Netflix/titus-control-plane)
[![Apache 2.0](https://img.shields.io/github/license/nebula-plugins/gradle-lint-plugin.svg)](http://www.apache.org/licenses/LICENSE-2.0)

## Overview

Titus is the Netflix Container Management Platform that manages containers and provides integrations to the infrastructure
ecosystem. This repository contains the control plane components which are responsible for accepting job requests and
scheduling containers on agents.

## Documentation & Getting Started

[netflix.github.io/titus](http://netflix.github.io/titus/)

## Building and Testing

For reference, check some of the scripts commonly used by committers in the `scripts/dev/` folder.

### Building

```sh-session
./gradlew build
```

### Run Tests

```sh-session
./gradlew test
```

### Run All Tests (including integration)

```sh-session
./gradlew testAll
```

### Test databases

By default, the database tests are executed with an embedded Postgres service. This can be modified by setting
database profiles in `~/.titus-jooq.yaml` file:
```yaml
profiles:
  relocation:
    name: relocation
    databaseUrl: jdbc:postgresql://localhost:5432/postgres
    user: titus
    password: 123
  jobActivityJooqContext:
    name: jobActivityJooqContext
    databaseUrl: jdbc:postgresql://localhost:5432/postgres
    user: postgres
    password: postgres
  producerJooqContext:
    name: producerJooqContext
    databaseUrl: jdbc:postgresql://localhost:5432/postgres
    user: postgres
    password: postgres
```

## Extensions

There are several extensions in the `titus-ext` folder for integrations with various systems. In order to use
these extensions, a wrapper project that reconfigures the guice bindings is needed. A tutorial project for binding
the different implementations is coming soon.

## Local testing with docker-compose

[`docker-compose`](https://docs.docker.com/compose/install/) together with [`docker-engine`](https://docs.docker.com/engine/)
can be used to stand up a local cluster with all components necessary to run titus containers. Each component
(titus-master, titus-gateway, mesos-master, zookeeper, titus-agent) will run as a separate docker container, and Titus
containers will be launched as nested containers (docker-in-docker) inside the `agent` container.

The last versions known to work:

- docker-engine `18.06.0-ce`
- docker-compose `1.22.0`

To build and launch all components:

```sh-session
docker-compose build
docker-compose up -d
```

Ports `7001` (http) and `7104` (grpc) from titus-gateway will be exposed locally:

```sh-session
curl localhost:7001/api/v2/status
curl localhost:7001/api/v2/leader
```

Logs can be inspected with `docker-compose logs`. For agent logs, `journald` can be
used in a shell inside the agent container:

```sh-session
docker-compose exec agent bash
(agent) $ journalctl

# Note: some warnings and errors are to be expected on those logs
# Not all systemd units will work when not on an EC2 VM

# list nested docker containers launched by Titus
(agent) $ docker ps
```

By default, all titus-agent containers will join a cluster named `unknown-instanceGroup`.
Before any tasks can be scheduled, that cluster needs to be activated. Note that
this is necessary _every time the Titus master is restarted_, since `instanceGroup`
state is not being persisted:

```sh-session
curl localhost:7001/api/v3/agent/instanceGroups/unknown-instanceGroup/lifecycle \
  -X PUT -H "Content-type: application/json" -d \
  '{"instanceGroupId": "unknown-instanceGroup", "lifecycleState": "Active"}'
```

If you get an error saying that `unknown-instanceGroup` does not exist, wait 10s and try again. Agents take some time to
get registered.

After that, jobs can be submitted normally. AWS integrations will not work in this mode:

```sh-session
curl localhost:7001/api/v3/jobs \
  -X POST -H "Content-type: application/json" -d \
  '{
    "applicationName": "localtest",
    "owner": {"teamEmail": "me@me.com"},
    "container": {
      "image": {"name": "alpine", "tag": "latest"},
      "entryPoint": ["/bin/sleep", "1h"],
      "securityProfile": {"iamRole": "test-role", "securityGroups": ["sg-test"]}
    },
    "batch": {
      "size": 1,
      "runtimeLimitSec": "3600",
      "retryPolicy":{"delayed": {"delayMs": "1000", "retries": 3}}
    }
  }'
```

The status of the new job, and its task (container) can be queried with:

```sh-session
curl localhost:7001/api/v3/jobs
curl localhost:7001/api/v3/tasks

# to delete a task or a job:
curl localhost:7001/api/v3/jobs/:id -X DELETE
curl localhost:7001/api/v3/tasks/:id -X DELETE
```

Components can also be built and deployed individually. E.g.:

```sh-session
# build and deploy an agent
docker-compose build agent
docker-compose up -d agent

# build and deploy titus-master
docker-compose build master
docker-compose up -d master
```

To add more agents to the cluster:

```sh-session
docker-compose up -d --scale agent=2
```

Note that it can take ~10s for a new titus-agent to be detected and registered with the default configuration.

After you are done:

```sh-session
# tear everything down
docker-compose down

# or only stop titus-master
docker-compose stop master
```

## LICENSE

Copyright (c) 2018 Netflix, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
