/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.testkit.perf.load.runner;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.simulator.TitusCloudSimulator;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

/**
 * Removes instance groups belonging to past sessions.
 */
@Singleton
public class AgentTerminator {

    private static final Logger logger = LoggerFactory.getLogger(AgentTerminator.class);

    private final ExecutionContext context;

    @Inject
    public AgentTerminator(ExecutionContext context) {
        this.context = context;
    }

    public void doClean() {
        List<AgentInstanceGroup> instanceGroups = context.getCachedAgentManagementClient().getInstanceGroups();

        logger.info("Removing agent partitions: {}", instanceGroups.stream().map(AgentInstanceGroup::getId).collect(Collectors.toList()));

        List<Flux<Void>> removeActions = instanceGroups.stream()
                .map(instanceGroup ->
                        context.getSimulatedCloudClient()
                                .removeInstanceGroup(TitusCloudSimulator.Id.newBuilder().setId(instanceGroup.getId()).build())
                                .flux()
                )
                .collect(Collectors.toList());

        Flux.merge(removeActions).timeout(Duration.ofSeconds(30)).blockLast();
    }
}
