/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.master.mesos.kubeapiserver.direct.env;

import java.util.Arrays;
import java.util.List;

/**
 * Aggregate container environment variables from many sources. Evaluation happens from left to right, with the
 * next item overriding entries from previous evaluations if there is a collision.
 */
public abstract class AggregatingContainerEnvFactory implements ContainerEnvFactory {

    final List<ContainerEnvFactory> factories;

    public AggregatingContainerEnvFactory(ContainerEnvFactory... factories) {
        this.factories = Arrays.asList(factories);
    }
}
