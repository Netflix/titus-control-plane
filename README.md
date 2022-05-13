# Titus Control Plane (ARCHIVED)

This repo has been archived and is no longer in active development.

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
  jobactivity:
    name: jobActivityJooqContext
    databaseUrl: jdbc:postgresql://localhost:5432/jobactivity
    user: postgres
    password: postgres
  activity:
    name: producerJooqContext
    databaseUrl: jdbc:postgresql://localhost:5432/jobactivity
    user: postgres
    password: postgres
```

## Extensions

There are several extensions in the `titus-ext` folder for integrations with various systems. In order to use
these extensions, a wrapper project that reconfigures the guice bindings is needed. A tutorial project for binding
the different implementations is coming soon.

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
