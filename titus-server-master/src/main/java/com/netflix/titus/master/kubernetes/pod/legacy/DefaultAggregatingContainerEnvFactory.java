/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.legacy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Singleton;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation aggregating container environment variables from multiple sources.
 * Evaluation happens from left to right, with the next item overriding entries from
 * previous evaluations if there is a collision.
 */
@Singleton
public class DefaultAggregatingContainerEnvFactory implements ContainerEnvFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAggregatingContainerEnvFactory.class);

    private static final String CONFLICT_COUNTER = "titus.aggregatingContainerEnv.conflict";

    private final Registry registry;

    private final Id conflictId;

    private final List<ContainerEnvFactory> orderedFactoryList;

    public DefaultAggregatingContainerEnvFactory(TitusRuntime titusRuntime, ContainerEnvFactory... containerEnvFactories) {
        orderedFactoryList = Arrays.asList(containerEnvFactories);
        this.registry = titusRuntime.getRegistry();
        this.conflictId = registry.createId(CONFLICT_COUNTER);
    }

    @Override
    public Pair<List<String>, Map<String, String>> buildContainerEnv(Job<?> job, Task task) {
        List<String> systemEnvNames = new ArrayList<>();
        Map<String, String> env = new HashMap<>();
        for (ContainerEnvFactory factory : orderedFactoryList) {
            Pair<List<String>, Map<String, String>> incomingContainerEnv = factory.buildContainerEnv(job, task);
            Map<String, String> incomingEnv = incomingContainerEnv.getRight();
            List<String> incomingSystemEnv = incomingContainerEnv.getLeft();
            // Tracking conflicting env var for any two given factories
            env.keySet()
                    .stream()
                    .filter(incomingEnv::containsKey)
                    .forEach(envVarName -> incrementConflictCounter(envVarName, job.getId(), job.getJobDescriptor().getApplicationName()));
            env.putAll(incomingEnv);
            systemEnvNames.addAll(incomingSystemEnv);
        }
        return Pair.of(systemEnvNames, env);
    }

    private void incrementConflictCounter(String envVarName, String jobId, String applicationName) {
        logger.info("JobId {} applicationName {} has conflicting env variable {}", jobId, applicationName, envVarName);
        registry.counter(this.conflictId.withTags("var_name", envVarName)).increment();
    }
}
