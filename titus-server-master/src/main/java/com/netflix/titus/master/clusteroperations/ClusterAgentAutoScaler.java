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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;
import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceOverrideState;
import com.netflix.titus.api.agent.model.InstanceOverrideStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.limiter.ImmutableLimiters;
import com.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket;
import com.netflix.titus.common.util.limiter.tokenbucket.ImmutableTokenBucket.ImmutableRefillStrategy;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.SchedulerExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.scheduler.SchedulingService;
import com.netflix.titus.master.scheduler.TaskPlacementFailure;
import com.netflix.titus.master.scheduler.TaskPlacementFailure.FailureKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Scheduler;
import rx.Subscription;

import static com.netflix.titus.master.MetricConstants.METRIC_CLUSTER_OPERATIONS;
import static com.netflix.titus.master.clusteroperations.ClusterOperationFunctions.canFit;
import static com.netflix.titus.master.clusteroperations.ClusterOperationFunctions.getNumberOfTasksOnAgents;
import static com.netflix.titus.master.clusteroperations.ClusterOperationFunctions.hasTimeElapsed;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * This component is responsible for adding and removing agents.
 */
@Singleton
public class ClusterAgentAutoScaler {
    private static final Logger logger = LoggerFactory.getLogger(ClusterAgentAutoScaler.class);
    private static final String METRIC_ROOT = METRIC_CLUSTER_OPERATIONS + "clusterAgentAutoScaler.";
    private static final long TIME_TO_WAIT_AFTER_ACTIVATION = 300_000;
    private static final long AUTO_SCALER_ITERATION_INTERVAL_MS = 30_000;
    private static final long CLUSTER_AGENT_AUTO_SCALE_COMPLETABLE_TIMEOUT_MS = 300_000;
    private static final long TASK_IDS_PREVIOUSLY_SCALED_TTL_MS = 600_000;
    private static final long SCALE_UP_TOKEN_BUCKET_CAPACITY = 50;
    private static final long SCALE_UP_TOKEN_BUCKET_REFILL_AMOUNT = 2;
    private static final long SCALE_UP_TOKEN_BUCKET_REFILL_INTERVAL_MS = 1_000;
    private static final long SCALE_DOWN_TOKEN_BUCKET_CAPACITY = 50;
    private static final long SCALE_DOWN_TOKEN_BUCKET_REFILL_AMOUNT = 2;
    private static final long SCALE_DOWN_TOKEN_BUCKET_REFILL_INTERVAL_MS = 1_000;

    private final TitusRuntime titusRuntime;
    private final ClusterOperationsConfiguration configuration;
    private final AgentManagementService agentManagementService;
    private final V3JobOperations v3JobOperations;
    private final SchedulingService schedulingService;
    private final Scheduler scheduler;
    private final Clock clock;
    private final Cache<String, String> taskIdsForPreviousScaleUps;
    private final Map<Tier, TierAutoScalerExecution> tierTierAutoScalerExecutions;

    private Subscription agentAutoScalerSubscription;

    @Inject
    public ClusterAgentAutoScaler(TitusRuntime titusRuntime,
                                  ClusterOperationsConfiguration configuration,
                                  AgentManagementService agentManagementService,
                                  V3JobOperations v3JobOperations,
                                  SchedulingService schedulingService) {
        this(titusRuntime, configuration, agentManagementService, v3JobOperations, schedulingService,
                SchedulerExt.createSingleThreadScheduler("cluster-auto-scaler"));
    }

    public ClusterAgentAutoScaler(TitusRuntime titusRuntime,
                                  ClusterOperationsConfiguration configuration,
                                  AgentManagementService agentManagementService,
                                  V3JobOperations v3JobOperations,
                                  SchedulingService schedulingService,
                                  Scheduler scheduler) {
        this.titusRuntime = titusRuntime;
        this.configuration = configuration;
        this.agentManagementService = agentManagementService;
        this.v3JobOperations = v3JobOperations;
        this.schedulingService = schedulingService;
        this.scheduler = scheduler;
        this.clock = titusRuntime.getClock();
        this.taskIdsForPreviousScaleUps = CacheBuilder.newBuilder()
                .expireAfterWrite(TASK_IDS_PREVIOUSLY_SCALED_TTL_MS, TimeUnit.MILLISECONDS)
                .build();
        this.tierTierAutoScalerExecutions = new HashMap<>();
    }

    @Activator
    public void enterActiveMode() {
        agentAutoScalerSubscription = ObservableExt.schedule(
                METRIC_CLUSTER_OPERATIONS + "clusterAgentAutoScaler", titusRuntime.getRegistry(),
                "doAgentScaling", doAgentScaling(),
                TIME_TO_WAIT_AFTER_ACTIVATION, AUTO_SCALER_ITERATION_INTERVAL_MS, TimeUnit.MILLISECONDS, scheduler
        ).subscribe(next -> next.ifPresent(e -> logger.warn("doAgentScaling error:", e)));
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(agentAutoScalerSubscription);
    }

    @VisibleForTesting
    Completable doAgentScaling() {
        return Completable.defer(() -> {
            if (!configuration.isAutoScalingAgentsEnabled()) {
                logger.debug("Auto scaling agents is not enabled");
                return Completable.complete();
            }

            List<Completable> actions = new ArrayList<>();

            Map<String, Job> allJobs = getAllJobs();
            Map<String, Task> allTasks = getAllTasks();
            List<AgentInstanceGroup> activeInstanceGroups = getActiveInstanceGroups();
            Map<AgentInstanceGroup, List<AgentInstance>> instancesForActiveInstanceGroups = getInstancesForInstanceGroups(activeInstanceGroups);
            Map<String, List<AgentInstance>> instancesForActiveInstanceGroupsById = instancesForActiveInstanceGroups.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().getId(), Map.Entry::getValue));
            Map<String, Long> numberOfTasksOnAgents = getNumberOfTasksOnAgents(allTasks.values());
            Map<FailureKind, List<TaskPlacementFailure>> lastTaskPlacementFailures = schedulingService.getLastTaskPlacementFailures();
            Map<String, TaskPlacementFailure> launchGuardFailuresByTaskId = getLaunchGuardFailuresByTaskId(lastTaskPlacementFailures);
            Map<Tier, Set<String>> failedTaskIdsByTier = getFailedTaskIds(lastTaskPlacementFailures);

            long now = clock.wallTime();

            for (Tier tier : Tier.values()) {
                logger.info("Starting scaling actions for tier: {}", tier);
                TierAutoScalingConfiguration tierConfiguration = ClusterOperationFunctions.getTierConfiguration(tier, configuration);
                TierAutoScalerExecution tierAutoScalerExecution = tierTierAutoScalerExecutions.computeIfAbsent(
                        tier, k -> new TierAutoScalerExecution(tier, titusRuntime.getRegistry())
                );
                // This will throw an exception if not properly configured
                ResourceDimension tierResourceDimension = agentManagementService.getResourceLimits(tierConfiguration.getPrimaryInstanceType());

                List<AgentInstanceGroup> activeInstanceGroupsForTier = activeInstanceGroups.stream()
                        .filter(ig -> ig.getTier() == tier)
                        .collect(Collectors.toList());
                logger.info("{} active instance groups({}): {}", tier, activeInstanceGroupsForTier.size(), activeInstanceGroupsForTier);

                List<AgentInstance> idleInstancesForTier = getIdleInstancesForTier(tier, tierConfiguration.getPrimaryInstanceType(),
                        instancesForActiveInstanceGroups, numberOfTasksOnAgents, now, tierConfiguration.getIdleInstanceGracePeriodMs());
                tierAutoScalerExecution.getTotalIdleInstancesGauge().set(idleInstancesForTier.size());
                logger.info("{} idle instances({}): {}", tier, idleInstancesForTier.size(), idleInstancesForTier);

                Set<String> failedTaskIds = failedTaskIdsByTier.getOrDefault(tier, Collections.emptySet());
                tierAutoScalerExecution.getTotalFailedTasksGauge().set(failedTaskIds.size());
                logger.info("{} failed tasks({}): {}", tier, failedTaskIds.size(), failedTaskIds);

                int agentCountToScaleUp = 0;
                Set<String> potentialTaskIdsForScaleUp = new HashSet<>();

                boolean usedScaleUpCooldown = false;
                if (hasTimeElapsed(tierAutoScalerExecution.getLastScaleUp().get(), now, tierConfiguration.getScaleUpCoolDownMs())) {
                    int minIdleForTier = tierConfiguration.getMinIdle();
                    if (idleInstancesForTier.size() < minIdleForTier) {
                        int instancesNeededForMinIdle = minIdleForTier - idleInstancesForTier.size();
                        logger.info("{} needs {} instances to satisfy min idle {}", tier, instancesNeededForMinIdle, minIdleForTier);
                        agentCountToScaleUp += instancesNeededForMinIdle;
                    }

                    Set<String> placementFailureTaskIds = getTaskIdsForTierWithoutLaunchGuardFailures(tier, lastTaskPlacementFailures, launchGuardFailuresByTaskId);
                    logger.info("{} had the placement excluding launch guard failures({}): {}", tier, placementFailureTaskIds.size(), placementFailureTaskIds);

                    Set<String> scalablePlacementFailureTaskIds = filterOutTaskIdsForScaling(placementFailureTaskIds, allJobs, allTasks, tierResourceDimension);
                    logger.info("{} had the scalable placement failures({}): {}", tier, scalablePlacementFailureTaskIds.size(), scalablePlacementFailureTaskIds);
                    potentialTaskIdsForScaleUp.addAll(scalablePlacementFailureTaskIds);

                    if (agentCountToScaleUp > 0 || !scalablePlacementFailureTaskIds.isEmpty()) {
                        usedScaleUpCooldown = true;
                    }
                }

                Set<String> tasksPastSlo = getTasksPastSlo(failedTaskIds, allTasks, now, tierConfiguration.getTaskSloMs());
                Set<String> scalableTasksPastSlo = filterOutTaskIdsForScaling(tasksPastSlo, allJobs, allTasks, tierResourceDimension);
                tierAutoScalerExecution.getTotalTasksPastSloGauge().set(scalableTasksPastSlo.size());
                logger.info("{} had tasks past slo({}): {}", tier, scalableTasksPastSlo.size(), scalableTasksPastSlo);
                potentialTaskIdsForScaleUp.addAll(scalableTasksPastSlo);

                Set<String> taskIdsForScaleUp = new HashSet<>();
                for (String taskId : potentialTaskIdsForScaleUp) {
                    boolean previouslyScaledFor = taskIdsForPreviousScaleUps.getIfPresent(taskId) != null;
                    if (!previouslyScaledFor) {
                        taskIdsForScaleUp.add(taskId);
                        taskIdsForPreviousScaleUps.put(taskId, taskId);
                    }
                }

                tierAutoScalerExecution.getTotalTasksForScaleUpGauge().set(taskIdsForScaleUp.size());
                logger.info("{} had tasks to scale up({}): {}", tier, taskIdsForScaleUp.size(), taskIdsForScaleUp);

                int agentScaleUpCountByDominantResource = calculateAgentScaleUpCountByDominantResource(taskIdsForScaleUp, allJobs, allTasks, tierResourceDimension);
                logger.info("{} needs {} instances based on dominant resource", tier, agentScaleUpCountByDominantResource);

                agentCountToScaleUp += agentScaleUpCountByDominantResource;
                logger.info("{} needs {} instances", tier, agentCountToScaleUp);
                tierAutoScalerExecution.getTotalAgentsToScaleUpGauge().set(agentCountToScaleUp);
                boolean scalingUp = false;

                if (agentCountToScaleUp > 0) {
                    long maxTokensToTake = Math.min(SCALE_UP_TOKEN_BUCKET_CAPACITY, agentCountToScaleUp);
                    Optional<Pair<Long, ImmutableTokenBucket>> takeOpt = tierAutoScalerExecution.getLastScaleUpTokenBucket().tryTake(1, maxTokensToTake);
                    if (takeOpt.isPresent()) {
                        Pair<Long, ImmutableTokenBucket> takePair = takeOpt.get();
                        tierAutoScalerExecution.setLastScaleUpTokenBucket(takePair.getRight());
                        long tokensAvailable = takePair.getLeft();
                        Pair<Integer, Completable> scaleUpPair = createScaleUpCompletable(activeInstanceGroupsForTier, (int) tokensAvailable);
                        actions.add(scaleUpPair.getRight());
                        Integer agentCountBeingScaled = scaleUpPair.getLeft();
                        tierAutoScalerExecution.getTotalAgentsBeingScaledUpGauge().set(agentCountBeingScaled);
                        if (agentCountBeingScaled > 0) {
                            logger.info("Attempting to scale up {} tier by {} agent instances", tier, agentCountBeingScaled);
                            scalingUp = true;
                            if (usedScaleUpCooldown) {
                                tierAutoScalerExecution.getLastScaleUp().set(clock.wallTime());
                            }
                        }
                    }
                }

                if (!scalingUp && hasTimeElapsed(tierAutoScalerExecution.getLastScaleDown().get(), now, tierConfiguration.getScaleDownCoolDownMs())) {
                    int agentCountToScaleDown = 0;
                    int maxIdleForTier = tierConfiguration.getMaxIdle();
                    if (idleInstancesForTier.size() > maxIdleForTier) {
                        int instancesNotNeededForMaxIdle = idleInstancesForTier.size() - maxIdleForTier;
                        logger.info("{} can remove {} instances to satisfy max idle {}", tier, instancesNotNeededForMaxIdle, maxIdleForTier);
                        agentCountToScaleDown += instancesNotNeededForMaxIdle;
                    }

                    tierAutoScalerExecution.getTotalAgentsToScaleDownGauge().set(agentCountToScaleDown);

                    if (agentCountToScaleDown > 0) {
                        long maxTokensToTake = Math.min(SCALE_DOWN_TOKEN_BUCKET_CAPACITY, agentCountToScaleDown);
                        Optional<Pair<Long, ImmutableTokenBucket>> takeOpt = tierAutoScalerExecution.getLastScaleDownTokenBucket().tryTake(1, maxTokensToTake);
                        if (takeOpt.isPresent()) {
                            Pair<Long, ImmutableTokenBucket> takePair = takeOpt.get();
                            tierAutoScalerExecution.setLastScaleDownTokenBucket(takePair.getRight());
                            long tokensAvailable = takePair.getLeft();
                            Pair<Integer, Completable> scaleDownPair = createSetRemovableOverrideStatusesCompletable(idleInstancesForTier,
                                    activeInstanceGroupsForTier, instancesForActiveInstanceGroupsById, (int) tokensAvailable);
                            actions.add(scaleDownPair.getRight());
                            Integer agentCountBeingScaledDown = scaleDownPair.getLeft();
                            tierAutoScalerExecution.getTotalAgentsBeingScaledDownGauge().set(agentCountBeingScaledDown);
                            if (agentCountBeingScaledDown > 0) {
                                logger.info("Attempting to scale down {} tier by {} agent instances", tier, agentCountBeingScaledDown);
                                tierAutoScalerExecution.getLastScaleDown().set(clock.wallTime());
                            }
                        }
                    }
                }
                logger.info("Finishing scaling actions for tier: {}", tier);
            }

            List<AgentInstance> removableInstancesPastElapsedTime = getRemovableInstancesPastElapsedTime(instancesForActiveInstanceGroups,
                    now, configuration.getAgentInstanceRemovableTimeoutMs());
            logger.info("Removable instances past elapsed time({}): {}", removableInstancesPastElapsedTime.size(), removableInstancesPastElapsedTime);

            if (!removableInstancesPastElapsedTime.isEmpty()) {
                actions.add(createResetOverrideStatusesCompletable(removableInstancesPastElapsedTime));
                logger.info("Resetting agent instances({}): {}", removableInstancesPastElapsedTime.size(), removableInstancesPastElapsedTime);
            }

            return Completable.concat(actions);
        }).doOnCompleted(() -> logger.debug("Completed scaling agents"))
                .timeout(CLUSTER_AGENT_AUTO_SCALE_COMPLETABLE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private List<AgentInstanceGroup> getActiveInstanceGroups() {
        return agentManagementService.getInstanceGroups().stream()
                .filter(ig -> ig.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Active)
                .collect(Collectors.toList());
    }

    private Map<AgentInstanceGroup, List<AgentInstance>> getInstancesForInstanceGroups(List<AgentInstanceGroup> instanceGroups) {
        Map<AgentInstanceGroup, List<AgentInstance>> instancesForActiveInstanceGroups = new HashMap<>();
        for (AgentInstanceGroup instanceGroup : instanceGroups) {
            List<AgentInstance> instances = instancesForActiveInstanceGroups.computeIfAbsent(instanceGroup, k -> new ArrayList<>());
            instances.addAll(agentManagementService.getAgentInstances(instanceGroup.getId()));
        }
        return instancesForActiveInstanceGroups;
    }

    private List<AgentInstance> getRemovableInstancesPastElapsedTime(Map<AgentInstanceGroup, List<AgentInstance>> instancesForActiveInstanceGroups,
                                                                     long finish,
                                                                     long elapsed) {
        return instancesForActiveInstanceGroups.entrySet().stream()
                .flatMap(e -> e.getValue().stream().filter(i -> {
                    InstanceOverrideStatus overrideStatus = i.getOverrideStatus();
                    return overrideStatus.getState() == InstanceOverrideState.Removable && hasTimeElapsed(overrideStatus.getTimestamp(), finish, elapsed);
                }))
                .collect(Collectors.toList());
    }

    private List<AgentInstance> getIdleInstancesForTier(Tier tier,
                                                        String primaryInstanceType,
                                                        Map<AgentInstanceGroup, List<AgentInstance>> instancesForActiveInstanceGroups,
                                                        Map<String, Long> numberOfTasksOnAgent,
                                                        long finished,
                                                        long elapsed) {
        return instancesForActiveInstanceGroups.entrySet().stream()
                .filter(e -> {
                    AgentInstanceGroup instanceGroup = e.getKey();
                    return instanceGroup.getTier() == tier && instanceGroup.getInstanceType().equals(primaryInstanceType);
                })
                .flatMap(e -> e.getValue().stream().filter(i -> {
                    InstanceLifecycleStatus lifecycleStatus = i.getLifecycleStatus();
                    InstanceOverrideStatus overrideStatus = i.getOverrideStatus();
                    return lifecycleStatus.getState() == InstanceLifecycleState.Started &&
                            hasTimeElapsed(lifecycleStatus.getLaunchTimestamp(), finished, elapsed) &&
                            overrideStatus.getState() == InstanceOverrideState.None &&
                            numberOfTasksOnAgent.getOrDefault(i.getId(), 0L) <= 0;
                }))
                .collect(Collectors.toList());
    }

    private Set<String> getTaskIdsForTierWithoutLaunchGuardFailures(Tier tier,
                                                                    Map<FailureKind, List<TaskPlacementFailure>> lastTaskPlacementFailures,
                                                                    Map<String, TaskPlacementFailure> launchGuardFailuresByTaskId) {
        Set<String> taskIds = new HashSet<>();
        for (List<TaskPlacementFailure> failures : lastTaskPlacementFailures.values()) {
            for (TaskPlacementFailure failure : failures) {
                if (failure.getTier() == tier && !launchGuardFailuresByTaskId.containsKey(failure.getTaskId())) {
                    taskIds.add(failure.getTaskId());
                }
            }
        }
        return taskIds;
    }


    private Pair<Integer, Completable> createScaleUpCompletable(List<AgentInstanceGroup> scalableInstanceGroups, int scaleUpCount) {
        int count = 0;
        List<Completable> actions = new ArrayList<>();
        for (AgentInstanceGroup instanceGroup : scalableInstanceGroups) {
            int totalAgentsNeeded = scaleUpCount - count;
            if (totalAgentsNeeded <= 0) {
                break;
            }
            int agentsAvailableInInstanceGroup = instanceGroup.getMax() - instanceGroup.getDesired();
            int agentsToScaleInInstanceGroup = Math.min(totalAgentsNeeded, agentsAvailableInInstanceGroup);
            actions.add(agentManagementService.scaleUp(instanceGroup.getId(), agentsToScaleInInstanceGroup));
            count += agentsToScaleInInstanceGroup;
        }
        return Pair.of(count, Completable.concat(actions));
    }

    private Pair<Integer, Completable> createSetRemovableOverrideStatusesCompletable(List<AgentInstance> idleInstances,
                                                                                     List<AgentInstanceGroup> scalableInstanceGroups,
                                                                                     Map<String, List<AgentInstance>> instancesForActiveInstanceGroupsById,
                                                                                     int scaleDownCount) {
        List<Completable> actions = new ArrayList<>();
        Map<String, List<AgentInstance>> idleInstancesByInstanceGroup = new HashMap<>();
        for (AgentInstance agentInstance : idleInstances) {
            List<AgentInstance> instances = idleInstancesByInstanceGroup.computeIfAbsent(agentInstance.getInstanceGroupId(), k -> new ArrayList<>());
            instances.add(agentInstance);
        }

        int count = 0;
        for (AgentInstanceGroup instanceGroup : scalableInstanceGroups) {
            int remainingAgentsToRemove = scaleDownCount - count;
            if (remainingAgentsToRemove <= 0) {
                break;
            }

            List<AgentInstance> removableInstancesInInstanceGroup = instancesForActiveInstanceGroupsById.getOrDefault(instanceGroup.getId(), emptyList())
                    .stream()
                    .filter(i -> i.getOverrideStatus().getState() == InstanceOverrideState.Removable)
                    .collect(Collectors.toList());

            List<AgentInstance> agentsEligibleToRemoveInInstanceGroup = idleInstancesByInstanceGroup.getOrDefault(instanceGroup.getId(), emptyList());
            int agentCountEligibleToRemoveInInstanceGroup = instanceGroup.getCurrent() - instanceGroup.getMin() - removableInstancesInInstanceGroup.size();
            int agentCountToRemoveInInstanceGroup = Ints.min(remainingAgentsToRemove, agentCountEligibleToRemoveInInstanceGroup, agentsEligibleToRemoveInInstanceGroup.size());
            for (int i = 0; i < agentCountToRemoveInInstanceGroup; i++) {
                AgentInstance agentInstance = agentsEligibleToRemoveInInstanceGroup.get(i);
                InstanceOverrideStatus removableOverrideStatus = InstanceOverrideStatus.newBuilder()
                        .withState(InstanceOverrideState.Removable)
                        .withTimestamp(clock.wallTime())
                        .withDetail("ClusterAgentAutoScaler setting to Removable to scale down the instance")
                        .build();
                Completable completable = agentManagementService.updateInstanceOverride(agentInstance.getId(), removableOverrideStatus);
                actions.add(completable);
                count++;
            }
        }
        return Pair.of(count, Completable.concat(actions));
    }

    private Completable createResetOverrideStatusesCompletable(List<AgentInstance> removableInstances) {
        List<Completable> actions = new ArrayList<>();
        for (AgentInstance agentInstance : removableInstances) {
            Completable completable = agentManagementService.removeInstanceOverride(agentInstance.getId());
            actions.add(completable);
        }
        return Completable.concat(actions);
    }


    private Map<String, Job> getAllJobs() {
        return v3JobOperations.getJobs().stream().collect(Collectors.toMap(Job::getId, Function.identity()));
    }

    private Map<String, Task> getAllTasks() {
        return v3JobOperations.getTasks().stream().collect(Collectors.toMap(Task::getId, Function.identity()));
    }

    private Map<Tier, Set<String>> getFailedTaskIds(Map<FailureKind, List<TaskPlacementFailure>> taskPlacementFailures) {
        Map<Tier, Set<String>> failedTaskIdsByTier = new HashMap<>();
        for (List<TaskPlacementFailure> failures : taskPlacementFailures.values()) {
            for (TaskPlacementFailure failure : failures) {
                Set<String> failedTaskIds = failedTaskIdsByTier.computeIfAbsent(failure.getTier(), k -> new HashSet<>());
                failedTaskIds.add(failure.getTaskId());
            }
        }
        return failedTaskIdsByTier;
    }

    private Set<String> getTasksPastSlo(Set<String> failedTaskIds, Map<String, Task> allTasks, long finish, long elapsed) {
        Set<String> taskIdsPastSlo = new HashSet<>();
        for (String taskId : failedTaskIds) {
            Task task = allTasks.get(taskId);
            if (task != null) {
                TaskStatus status = task.getStatus();
                if (status.getState() == TaskState.Accepted && hasTimeElapsed(status.getTimestamp(), finish, elapsed)) {
                    taskIdsPastSlo.add(taskId);
                }
            }
        }
        return taskIdsPastSlo;
    }

    private Map<String, TaskPlacementFailure> getLaunchGuardFailuresByTaskId(Map<FailureKind, List<TaskPlacementFailure>> lastTaskPlacementFailures) {
        return lastTaskPlacementFailures.getOrDefault(FailureKind.LaunchGuard, Collections.emptyList()).stream()
                .collect(Collectors.toMap(TaskPlacementFailure::getTaskId, Function.identity()));
    }

    private Set<String> filterOutTaskIdsForScaling(Set<String> taskIds,
                                                   Map<String, Job> allJobs,
                                                   Map<String, Task> allTasks,
                                                   ResourceDimension resourceDimension) {
        Set<String> filteredTaskIds = new HashSet<>();
        for (String taskId : taskIds) {
            ContainerResources taskContainerResources = getTaskContainerResources(taskId, allJobs, allTasks);
            if (taskContainerResources != null && canFit(taskContainerResources, resourceDimension)) {
                filteredTaskIds.add(taskId);
            }
        }
        return filteredTaskIds;
    }

    private int calculateAgentScaleUpCountByDominantResource(Set<String> taskIds,
                                                             Map<String, Job> allJobs,
                                                             Map<String, Task> allTasks,
                                                             ResourceDimension resourceDimension) {
        double totalCpus = 0;
        double totalMemoryMB = 0;
        double totalDiskMB = 0;
        double totalNetworkMbps = 0;
        for (String taskId : taskIds) {
            ContainerResources taskContainerResources = getTaskContainerResources(taskId, allJobs, allTasks);
            if (taskContainerResources != null) {
                totalCpus += taskContainerResources.getCpu();
                totalMemoryMB += taskContainerResources.getMemoryMB();
                totalDiskMB += taskContainerResources.getDiskMB();
                totalNetworkMbps += taskContainerResources.getNetworkMbps();
            }
        }

        int instancesByCpu = (int) Math.ceil(totalCpus / resourceDimension.getCpu());
        int instancesByMemory = (int) Math.ceil(totalMemoryMB / (double) resourceDimension.getMemoryMB());
        int instancesByDisk = (int) Math.ceil(totalDiskMB / (double) resourceDimension.getDiskMB());
        int instancesByNetwork = (int) Math.ceil(totalNetworkMbps / (double) resourceDimension.getNetworkMbs());

        return Ints.max(instancesByCpu, instancesByMemory, instancesByDisk, instancesByNetwork);
    }

    private ContainerResources getTaskContainerResources(String taskId, Map<String, Job> allJobs, Map<String, Task> allTasks) {
        Task task = allTasks.get(taskId);
        if (task == null) {
            return null;
        }
        Job job = allJobs.get(task.getJobId());
        if (job == null) {
            return null;
        }
        return job.getJobDescriptor().getContainer().getContainerResources();
    }

    private static class TierAutoScalerExecution {
        private final AtomicLong lastScaleUp = new AtomicLong();
        private final AtomicLong lastScaleDown = new AtomicLong();

        private final Gauge totalIdleInstancesGauge;
        private final Gauge totalFailedTasksGauge;
        private final Gauge totalTasksPastSloGauge;
        private final Gauge totalTasksForScaleUpGauge;
        private final Gauge totalAgentsToScaleUpGauge;
        private final Gauge totalAgentsBeingScaledUpGauge;
        private final Gauge totalAgentsToScaleDownGauge;
        private final Gauge totalAgentsBeingScaledDownGauge;

        private ImmutableTokenBucket lastScaleUpTokenBucket;
        private ImmutableTokenBucket lastScaleDownTokenBucket;

        TierAutoScalerExecution(Tier tier, Registry registry) {
            List<Tag> commonTags = singletonList(new BasicTag("tier", tier.name()));
            totalIdleInstancesGauge = registry.gauge(METRIC_ROOT + "totalIdleInstances", commonTags);
            totalFailedTasksGauge = registry.gauge(METRIC_ROOT + "totalFailedTasks", commonTags);
            totalTasksPastSloGauge = registry.gauge(METRIC_ROOT + "totalTasksPastSlo", commonTags);
            totalTasksForScaleUpGauge = registry.gauge(METRIC_ROOT + "totalTasksForScaleUp", commonTags);
            totalAgentsToScaleUpGauge = registry.gauge(METRIC_ROOT + "totalAgentsToScaleUp", commonTags);
            totalAgentsBeingScaledUpGauge = registry.gauge(METRIC_ROOT + "totalAgentsBeingScaledUp", commonTags);
            totalAgentsToScaleDownGauge = registry.gauge(METRIC_ROOT + "totalAgentsToScaleDown", commonTags);
            totalAgentsBeingScaledDownGauge = registry.gauge(METRIC_ROOT + "totalAgentsBeingScaledDown", commonTags);

            ImmutableRefillStrategy scaleUpRefillStrategy = ImmutableLimiters.refillAtFixedInterval(SCALE_UP_TOKEN_BUCKET_REFILL_AMOUNT,
                    SCALE_UP_TOKEN_BUCKET_REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            lastScaleUpTokenBucket = ImmutableLimiters.tokenBucket(SCALE_UP_TOKEN_BUCKET_CAPACITY, scaleUpRefillStrategy);

            ImmutableRefillStrategy scaleDownRefillStrategy = ImmutableLimiters.refillAtFixedInterval(SCALE_DOWN_TOKEN_BUCKET_REFILL_AMOUNT,
                    SCALE_DOWN_TOKEN_BUCKET_REFILL_INTERVAL_MS, TimeUnit.MILLISECONDS);
            lastScaleDownTokenBucket = ImmutableLimiters.tokenBucket(SCALE_DOWN_TOKEN_BUCKET_CAPACITY, scaleDownRefillStrategy);
        }

        AtomicLong getLastScaleUp() {
            return lastScaleUp;
        }

        AtomicLong getLastScaleDown() {
            return lastScaleDown;
        }

        Gauge getTotalIdleInstancesGauge() {
            return totalIdleInstancesGauge;
        }

        Gauge getTotalFailedTasksGauge() {
            return totalFailedTasksGauge;
        }

        Gauge getTotalTasksPastSloGauge() {
            return totalTasksPastSloGauge;
        }

        Gauge getTotalTasksForScaleUpGauge() {
            return totalTasksForScaleUpGauge;
        }

        Gauge getTotalAgentsToScaleUpGauge() {
            return totalAgentsToScaleUpGauge;
        }

        Gauge getTotalAgentsBeingScaledUpGauge() {
            return totalAgentsBeingScaledUpGauge;
        }

        Gauge getTotalAgentsToScaleDownGauge() {
            return totalAgentsToScaleDownGauge;
        }

        Gauge getTotalAgentsBeingScaledDownGauge() {
            return totalAgentsBeingScaledDownGauge;
        }

        ImmutableTokenBucket getLastScaleUpTokenBucket() {
            return lastScaleUpTokenBucket;
        }

        ImmutableTokenBucket getLastScaleDownTokenBucket() {
            return lastScaleDownTokenBucket;
        }

        void setLastScaleUpTokenBucket(ImmutableTokenBucket lastScaleUpTokenBucket) {
            this.lastScaleUpTokenBucket = lastScaleUpTokenBucket;
        }

        void setLastScaleDownTokenBucket(ImmutableTokenBucket lastScaleDownTokenBucket) {
            this.lastScaleDownTokenBucket = lastScaleDownTokenBucket;
        }
    }
}
