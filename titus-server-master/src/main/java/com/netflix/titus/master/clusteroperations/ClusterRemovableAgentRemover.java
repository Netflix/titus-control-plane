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

package com.netflix.titus.master.clusteroperations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceOverrideState;
import com.netflix.titus.api.agent.model.InstanceOverrideStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.limiter.ImmutableLimiters;
import com.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import com.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket.ImmutableRefillStrategy;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

import static com.netflix.titus.master.MetricConstants.METRIC_CLUSTER_OPERATIONS;

/**
 * This component is responsible for removing agents in a Removable Instance Group.
 */
@Singleton
public class ClusterRemovableAgentRemover {
    private static final Logger logger = LoggerFactory.getLogger(ClusterRemovableAgentRemover.class);
    private static final String METRIC_ROOT = METRIC_CLUSTER_OPERATIONS + "clusterRemovableAgentRemover.";
    private static final long TIME_TO_WAIT_AFTER_ACTIVATION = 300_000;
    private static final long REMOVE_AGENTS_ITERATION_INTERVAL_MS = 30_000;
    private static final long REMOVE_AGENTS_COMPLETABLE_TIMEOUT_MS = 300_000;
    private static final long AGENT_INSTANCES_BEING_TERMINATED_TTL_MS = 300_000;
    private static final long TOKEN_BUCKET_CAPACITY = 100;
    private static final long TOKEN_BUCKET_REFILL_AMOUNT = 2;
    private static final long TOKEN_BUCKET_REFILL_INTERVAL_MS = 1_000;

    private final TitusRuntime titusRuntime;
    private final ClusterOperationsConfiguration configuration;
    private final AgentManagementService agentManagementService;
    private final V3JobOperations v3JobOperations;
    private final Scheduler scheduler;
    private final Cache<String, String> agentInstancesBeingTerminated;
    private final Gauge totalAgentsToRemoveGauge;
    private final Gauge totalEligibleAgentsToRemoveGauge;
    private final Gauge totalAgentsBeingRemovedGauge;

    private ImmutableTokenBucket lastTokenBucket;
    private Subscription removeAgentsSubscription;

    @Inject
    public ClusterRemovableAgentRemover(TitusRuntime titusRuntime,
                                        ClusterOperationsConfiguration configuration,
                                        AgentManagementService agentManagementService,
                                        V3JobOperations v3JobOperations) {
        this(titusRuntime, configuration, agentManagementService, v3JobOperations, Schedulers.newThread());
    }

    public ClusterRemovableAgentRemover(TitusRuntime titusRuntime,
                                        ClusterOperationsConfiguration configuration,
                                        AgentManagementService agentManagementService,
                                        V3JobOperations v3JobOperations,
                                        Scheduler scheduler) {
        this.titusRuntime = titusRuntime;
        this.configuration = configuration;
        this.agentManagementService = agentManagementService;
        this.v3JobOperations = v3JobOperations;
        this.scheduler = scheduler;
        this.agentInstancesBeingTerminated = CacheBuilder.newBuilder()
                .expireAfterWrite(AGENT_INSTANCES_BEING_TERMINATED_TTL_MS, TimeUnit.MILLISECONDS)
                .build();

        Registry registry = titusRuntime.getRegistry();
        totalAgentsToRemoveGauge = registry.gauge(METRIC_ROOT + "totalAgentsToRemove");
        totalEligibleAgentsToRemoveGauge = registry.gauge(METRIC_ROOT + "totalEligibleAgentsToRemove");
        totalAgentsBeingRemovedGauge = registry.gauge(METRIC_ROOT + "totalAgentsBeingRemoved");
        createTokenBucket();
    }

    @Activator
    public void enterActiveMode() {
        this.removeAgentsSubscription = ObservableExt.schedule(
                METRIC_ROOT, titusRuntime.getRegistry(),
                "doRemoveAgents", doRemoveAgents(),
                TIME_TO_WAIT_AFTER_ACTIVATION, REMOVE_AGENTS_ITERATION_INTERVAL_MS, TimeUnit.MILLISECONDS, scheduler
        ).subscribe(next -> next.ifPresent(e -> logger.warn("doRemoveAgents error:", e)));
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(removeAgentsSubscription);
    }

    @VisibleForTesting
    Completable doRemoveAgents() {
        return Completable.defer(() -> {
            if (!configuration.isRemovingAgentsEnabled()) {
                logger.debug("Removing agents is not enabled");
                return Completable.complete();
            }

            long now = titusRuntime.getClock().wallTime();
            List<AgentInstanceGroup> eligibleInstanceGroups = agentManagementService.getInstanceGroups().stream()
                    .filter(ig -> ig.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Active ||
                            ig.getLifecycleStatus().getState() == InstanceGroupLifecycleState.PhasedOut)
                    .collect(Collectors.toList());

            logger.debug("Eligible instance groups: {}", eligibleInstanceGroups);
            long totalAgentsToRemove = 0;
            Map<AgentInstanceGroup, List<AgentInstance>> agentInstancesPerInstanceGroup = new HashMap<>();
            for (AgentInstanceGroup instanceGroup : eligibleInstanceGroups) {
                List<AgentInstance> agentInstances = agentManagementService.getAgentInstances(instanceGroup.getId())
                        .stream()
                        .filter(ig -> ig.getOverrideStatus().getState() == InstanceOverrideState.Removable)
                        .filter(ig -> now - ig.getOverrideStatus().getTimestamp() >= configuration.getAgentInstanceRemovableGracePeriodMs())
                        .collect(Collectors.toList());
                totalAgentsToRemove += agentInstances.size();
                agentInstancesPerInstanceGroup.put(instanceGroup, agentInstances);
            }
            totalAgentsToRemoveGauge.set(totalAgentsToRemove);
            logger.debug("Agent instances per instance group: {}", agentInstancesPerInstanceGroup);

            if (totalAgentsToRemove <= 0) {
                totalEligibleAgentsToRemoveGauge.set(0);
                return Completable.complete();
            }

            long totalEligibleAgentsToRemove = 0;
            Map<AgentInstanceGroup, List<AgentInstance>> eligibleAgentInstancesPerInstanceGroup = new HashMap<>();
            Map<String, Long> numberOfTasksOnAgents = getNumberOfTasksOnAgents();
            logger.debug("numberOfTasksOnAgents: {}", numberOfTasksOnAgents);

            for (Map.Entry<AgentInstanceGroup, List<AgentInstance>> entry : agentInstancesPerInstanceGroup.entrySet()) {
                List<AgentInstance> eligibleAgentInstances = entry.getValue().stream()
                        .filter(i -> agentInstancesBeingTerminated.getIfPresent(i.getId()) == null)
                        .filter(i -> numberOfTasksOnAgents.getOrDefault(i.getId(), 0L) <= 0)
                        .collect(Collectors.toList());
                totalEligibleAgentsToRemove += eligibleAgentInstances.size();
                eligibleAgentInstancesPerInstanceGroup.put(entry.getKey(), eligibleAgentInstances);
            }
            totalEligibleAgentsToRemoveGauge.set(totalEligibleAgentsToRemove);
            logger.debug("Eligible agent instances per instance group: {}", eligibleAgentInstancesPerInstanceGroup);

            if (totalEligibleAgentsToRemove <= 0) {
                return Completable.complete();
            }

            long maxTokensToTake = Math.min(TOKEN_BUCKET_CAPACITY, totalEligibleAgentsToRemove);
            Optional<Pair<Long, ImmutableTokenBucket>> takeOpt = this.lastTokenBucket.tryTake(1, maxTokensToTake);
            if (takeOpt.isPresent()) {
                Pair<Long, ImmutableTokenBucket> takePair = takeOpt.get();
                this.lastTokenBucket = takePair.getRight();
                long tokensAvailable = takePair.getLeft();
                long tokensUsed = 0;

                logger.debug("Attempting to terminate {} agent instances", tokensAvailable);
                List<Completable> actions = new ArrayList<>();
                for (Map.Entry<AgentInstanceGroup, List<AgentInstance>> entry : eligibleAgentInstancesPerInstanceGroup.entrySet()) {
                    long tokensRemaining = tokensAvailable - tokensUsed;
                    if (tokensRemaining <= 0) {
                        break;
                    }
                    List<AgentInstance> agentInstances = entry.getValue();
                    if (agentInstances.isEmpty()) {
                        continue;
                    }
                    String instanceGroupId = entry.getKey().getId();
                    List<AgentInstance> agentInstancesToTerminate = agentInstances.size() > tokensAvailable ? agentInstances.subList(0, (int) tokensRemaining) : agentInstances;
                    List<String> agentInstanceIdsToTerminate = agentInstancesToTerminate.stream().map(AgentInstance::getId).collect(Collectors.toList());
                    logger.info("Terminating in instance group: {} agent instances({}): {}", instanceGroupId, agentInstanceIdsToTerminate.size(), agentInstanceIdsToTerminate);
                    actions.add(createTerminateAgentsCompletable(instanceGroupId, agentInstanceIdsToTerminate));
                    tokensUsed += agentInstanceIdsToTerminate.size();
                }
                totalAgentsBeingRemovedGauge.set(tokensUsed);
                return Completable.concat(actions);
            }
            return Completable.complete();
        }).doOnCompleted(() -> logger.debug("Completed cluster agent removal"))
                .timeout(REMOVE_AGENTS_COMPLETABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }


    private void createTokenBucket() {
        ImmutableRefillStrategy immutableRefillStrategy = ImmutableLimiters.refillAtFixedInterval(TOKEN_BUCKET_REFILL_AMOUNT,
                TOKEN_BUCKET_REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        this.lastTokenBucket = ImmutableLimiters.tokenBucket(TOKEN_BUCKET_CAPACITY, immutableRefillStrategy);
    }

    private Completable createTerminateAgentsCompletable(String instanceGroupId, List<String> terminateIds) {
        Stopwatch timer = Stopwatch.createStarted();
        terminateIds.forEach(terminateId -> agentInstancesBeingTerminated.put(terminateId, terminateId));
        return agentManagementService.terminateAgents(instanceGroupId, terminateIds, true)
                .doOnNext(result -> {
                    if (result.size() != terminateIds.size()) {
                        titusRuntime.getCodeInvariants()
                                .inconsistent("Result collection size for instance group: %s does not match size of the terminate id collection: size(%s) != size(%s)",
                                        instanceGroupId, result, terminateIds);
                        return;
                    }
                    List<String> terminatedOk = new ArrayList<>();
                    Map<String, String> errors = new HashMap<>();
                    for (int i = 0; i < terminateIds.size(); i++) {
                        String instanceId = terminateIds.get(i);
                        Either<Boolean, Throwable> resultItem = result.get(i);
                        if (resultItem.hasValue()) {
                            if (resultItem.getValue()) {
                                terminatedOk.add(instanceId);
                            } else {
                                errors.put(instanceId, "Terminate status 'false'");
                            }
                        } else {
                            errors.put(instanceId, resultItem.getError().getMessage());
                        }
                    }
                    if (!terminatedOk.isEmpty()) {
                        logger.info("Successfully terminated agent instances of the instance group {}: {}", instanceGroupId, terminatedOk);
                    }
                    if (!errors.isEmpty()) {
                        logger.warn("Failed to terminate agent instances of the instance group {}: {}", instanceGroupId, errors);
                    }
                }).doOnError(e -> logger.warn("Failed to terminate agent instances {} belonging to the instance group {} after {}ms",
                        terminateIds, instanceGroupId, timer.elapsed(TimeUnit.MILLISECONDS), e)).toCompletable();
    }

    private Map<String, Long> getNumberOfTasksOnAgents() {
        return v3JobOperations.getTasks().stream()
                .collect(Collectors.groupingBy(
                        task -> task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_AGENT_ID, "Unknown"),
                        Collectors.counting())
                );
    }
}