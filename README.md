# Titus Control Plane
## Overview
Titus is the Netflix Cloud Container Runtime that manages containers and provides integrations to the infrastructure
ecosystem. This repository contains the control plane components which are responsible for accepting requests and
scheduling those requests on agents.

## Local testing with docker-compose

[`docker-compose`](https://docs.docker.com/compose/install/) together with [`docker-engine`](https://docs.docker.com/engine/)
can be used to stand up a local cluster with all components necessary to run titus containers. Each component
(titus-master, titus-gateway, mesos-master, zookeeper, titus-agent) will run as a separate docker container, and Titus
containers will be launched as nested containers (docker-in-docker) inside the `agent` container.

The last versions known to work:

* docker-engine `18.03.0-ce`
* docker-compose `1.21.0`

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
this is necessary *every time the Titus master is restarted*, since `instanceGroup`
state is not being persisted:

```
curl localhost:7001/api/v3/agent/instanceGroups/unknown-instanceGroup/lifecycle \
  -X PUT -H "Content-type: application/json" -d \
  '{"instanceGroupId": "unknown-instanceGroup", "lifecycleState": "Active"}'
```

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
docker-compose scale agent=2
```

Note that it can take ~10s for a new titus-agent to be detected and registered with the default configuration.

To tear everything down or to stop individual components:

```
docker-compose down
docker-compose stop master
```
