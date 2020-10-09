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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

/**
 * Aggregate container environment variables from many sources. Evaluation happens from left to right, with the
 * next item overriding entries from previous evaluations if there is a collision.
 */
public class AggregatingContainerEnvFactory implements ContainerEnvFactory {

    private final List<ContainerEnvFactory> factories;

    public AggregatingContainerEnvFactory(ContainerEnvFactory... factories) {
        this.factories = Arrays.asList(factories);
    }

    @Override
    public Map<String, String> buildContainerEnv(Job<?> job, Task task) {
        Map<String, String> env = new HashMap<>();
        for (ContainerEnvFactory factory : factories) {
            env.putAll(factory.buildContainerEnv(job, task));
        }
        return env;
    }
}
