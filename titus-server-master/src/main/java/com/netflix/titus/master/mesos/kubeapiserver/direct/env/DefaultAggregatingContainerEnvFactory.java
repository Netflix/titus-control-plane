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

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;

@Singleton
public class DefaultAggregatingContainerEnvFactory extends AggregatingContainerEnvFactory {

    private static final String CONFLICT_COUNTER = "titus.aggregatingContainerEnv.conflict";

    private final Registry registry;

    private final Id conflictId;

    @Inject
    public DefaultAggregatingContainerEnvFactory(TitusRuntime titusRuntime) {
        super(UserProvidedContainerEnvFactory.getInstance(), TitusProvidedContainerEnvFactory.getInstance());
        this.registry = titusRuntime.getRegistry();
        this.conflictId = registry.createId(CONFLICT_COUNTER);
    }

    @Override
    public Map<String, String> buildContainerEnv(Job<?> job, Task task) {
        Map<String, String> env = new HashMap<>();
        for (ContainerEnvFactory factory : factories) {
            Map<String, String> envMap = factory.buildContainerEnv(job, task);
            // Tracking conflicting env var for any two given factories
            env.keySet().stream().filter(envMap::containsKey).forEach(this::incrementConflictCounter);
            env.putAll(envMap);
        }
        return env;
    }

    private void incrementConflictCounter(String envVarName) {
        this.registry.counter(this.conflictId.withTags("env", envVarName)).increment();
    }
}
