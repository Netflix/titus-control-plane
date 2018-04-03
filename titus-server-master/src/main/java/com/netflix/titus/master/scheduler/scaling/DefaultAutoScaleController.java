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

package com.netflix.titus.master.scheduler.scaling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Stopwatch;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementFunctions;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.model.event.AutoScaleEvent;
import com.netflix.titus.api.model.event.FailedScaleDownEvent;
import com.netflix.titus.api.model.event.FailedScaleUpEvent;
import com.netflix.titus.api.model.event.ScaleDownEvent;
import com.netflix.titus.api.model.event.ScaleUpEvent;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

@Singleton
public class DefaultAutoScaleController implements AutoScaleController {

    private static final Logger logger = LoggerFactory.getLogger(DefaultAutoScaleController.class);

    private final AgentManagementService agentManagementService;

    private final Subject<AutoScaleEvent, AutoScaleEvent> eventSubject = new SerializedSubject<>(PublishSubject.create());
    private final AutoScaleControllerMetrics metrics;

    @Inject
    public DefaultAutoScaleController(AgentManagementService agentManagementService, Registry registry) {
        this.agentManagementService = agentManagementService;
        this.metrics = new AutoScaleControllerMetrics(registry);
    }

    @PreDestroy
    public void shutdown() {
        eventSubject.onCompleted();
    }

    @Override
    public void handleScaleUpAction(String instanceGroupName, int scaleUpCount) {
        AgentInstanceGroup instanceGroup;
        try {
            instanceGroup = agentManagementService.getInstanceGroup(instanceGroupName);
        } catch (Exception e) {
            metrics.unknownInstanceGroupScaleUp(instanceGroupName);
            logger.warn("Received a request to scale up an unknown instance group {}", instanceGroupName);
            return;
        }

        if (!canScaleUp(instanceGroup)) {
            metrics.scaleUpNotAllowed(instanceGroupName);
            return;
        }

        int newDesired = computeNewDesiredSize(scaleUpCount, instanceGroup);
        if (newDesired == instanceGroup.getDesired()) {
            logger.info("Instance group {} computed new desired size equal to the current one ({})", instanceGroupName, newDesired);
            metrics.scaleUpToExistingDesiredSize(instanceGroupName);
        } else {
            int delta = newDesired - instanceGroup.getDesired();
            logger.info("Changing instance group {} desired size from {} to {} (scale up by {})", instanceGroupName, instanceGroup.getDesired(), newDesired, delta);

            Stopwatch timer = Stopwatch.createStarted();
            agentManagementService.scaleUp(instanceGroupName, delta).subscribe(
                    () -> {
                        logger.info("Instance group {} desired size changed to {} in {}ms", instanceGroupName, newDesired, timer.elapsed(TimeUnit.MILLISECONDS));
                        eventSubject.onNext(new ScaleUpEvent(instanceGroupName, instanceGroup.getDesired(), newDesired));
                        metrics.scaleUpSucceeded(instanceGroupName);
                    },
                    e -> {
                        logger.info("Failed to change instance group {} desired size to {} after {}ms", instanceGroupName, newDesired, timer.elapsed(TimeUnit.MILLISECONDS), e);
                        eventSubject.onNext(new FailedScaleUpEvent(instanceGroupName, instanceGroup.getDesired(), newDesired, e.getMessage()));
                        metrics.scaleUpFailed(instanceGroupName, e);
                    }
            );
        }
    }

    @Override
    public Pair<Set<String>, Set<String>> handleScaleDownAction(String instanceGroupName, Set<String> instanceIds) {
        AgentInstanceGroup instanceGroup;
        List<AgentInstance> agentInstances;
        try {
            instanceGroup = agentManagementService.getInstanceGroup(instanceGroupName);
            agentInstances = agentManagementService.getAgentInstances(instanceGroupName);
        } catch (Exception e) {
            logger.warn("Received a request to terminate instance of unknown instance group {}", instanceGroupName);
            metrics.unknownInstanceGroupScaleDown(instanceGroupName);
            return Pair.of(Collections.emptySet(), instanceIds);
        }

        if (!canScaleDown(instanceGroup)) {
            metrics.scaleDownNotAllowed(instanceGroupName);
            return Pair.of(Collections.emptySet(), instanceIds);
        }

        Pair<Set<String>, Set<String>> resultPair = applyTerminateConstraints(instanceGroup, agentInstances, instanceIds);
        List<String> terminateIds = new ArrayList<>(resultPair.getLeft());
        Set<String> unknownInstances = resultPair.getRight();

        if (terminateIds.isEmpty()) {
            logger.info("No instances eligible to terminate in instance group {}", instanceGroupName);
            metrics.noInstancesToScaleDown(instanceGroup);
        } else {
            logger.warn("Terminating instances of the instance group {}: {}", instanceGroupName, terminateIds);

            Stopwatch timer = Stopwatch.createStarted();
            agentManagementService.terminateAgents(instanceGroupName, terminateIds, true).take(1).toSingle().subscribe(
                    result -> {
                        if (result.size() != terminateIds.size()) {
                            logger.warn("Result collection size does not match size of the terminate id collection: size({}) != size({})", result, terminateIds);
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
                            logger.info("Terminated successfully instances of the instance group {}: {}", instanceGroupName, terminatedOk);
                        }
                        if (!errors.isEmpty()) {
                            logger.warn("Failed to terminate instances of the instance group {}: {}", instanceGroupName, errors);
                        }

                        if (errors.isEmpty()) {
                            eventSubject.onNext(new ScaleDownEvent(instanceGroupName, new HashSet<>(terminatedOk)));
                        } else {
                            eventSubject.onNext(new FailedScaleDownEvent(instanceGroupName, new HashSet<>(terminatedOk), errors));
                        }

                        metrics.scaleDownFinished(instanceGroupName, terminatedOk.size(), errors.size());
                    },
                    e -> {
                        logger.warn("Failed to terminate instances {} belonging to the instance group {} after {}ms",
                                terminateIds, instanceGroupName, timer.elapsed(TimeUnit.MILLISECONDS), e);
                        eventSubject.onNext(new FailedScaleDownEvent(instanceGroupName, e.getMessage()));
                        metrics.scaleDownFailed(instanceGroupName, e);
                    }
            );
        }
        return Pair.of(new HashSet<>(terminateIds), unknownInstances);
    }

    @Override
    public Observable<AutoScaleEvent> events() {
        return eventSubject.asObservable();
    }

    private boolean canScaleUp(AgentInstanceGroup instanceGroup) {
        String instanceGroupName = instanceGroup.getId();

        if (!instanceGroup.isLaunchEnabled()) {
            logger.warn("Launch disabled for instance group {}", instanceGroupName);
            return false;
        }

        if (!AgentManagementFunctions.isActiveOrPhasedOut(instanceGroup)) {
            logger.warn("Instance group {} is not active or phased out (current state={}), but scale up requested",
                    instanceGroupName, instanceGroup.getLifecycleStatus().getState());
            return false;
        }
        return true;
    }

    private boolean canScaleDown(AgentInstanceGroup instanceGroup) {
        String instanceGroupName = instanceGroup.getId();
        InstanceGroupLifecycleState currentState = instanceGroup.getLifecycleStatus().getState();
        if (currentState == InstanceGroupLifecycleState.Inactive) {
            logger.warn("Instance group {} is in Inactive state, in which instances cannot be terminated", instanceGroupName);
            return false;
        }
        return true;
    }

    private int computeNewDesiredSize(int scaleUpCount, AgentInstanceGroup instanceGroup) {
        String instanceGroupName = instanceGroup.getId();
        int requestedDesired = instanceGroup.getDesired() + scaleUpCount;

        // Check instance group size constraint
        int alignedWithGroupMax = Math.min(requestedDesired, instanceGroup.getMax());
        if (requestedDesired != alignedWithGroupMax) {
            logger.warn("Instance group {} requested size {} is bigger than max={}. Changing it to {}",
                    instanceGroupName, requestedDesired, instanceGroup.getMax(), alignedWithGroupMax);
        }

        // Check autoscale rule constraint
        int alignedWithRuleMax = Math.min(alignedWithGroupMax, instanceGroup.getAutoScaleRule().getMax());
        if (alignedWithRuleMax != alignedWithGroupMax) {
            logger.warn("Instance group {} new desired size {} is bigger than autoscale rule max={}. Changing it to {}",
                    instanceGroupName, alignedWithGroupMax, instanceGroup.getAutoScaleRule().getMax(), alignedWithRuleMax);
        }

        return alignedWithRuleMax;
    }

    private Pair<Set<String>, Set<String>> applyTerminateConstraints(AgentInstanceGroup instanceGroup, List<AgentInstance> agentInstances, Set<String> candidateIds) {
        // Get rid of unknown instances
        Map<String, AgentInstance> id2InstanceMap = agentInstances.stream().collect(Collectors.toMap(AgentInstance::getId, Function.identity()));
        List<String> terminateIds = new ArrayList<>();
        Set<String> unknownInstances = new HashSet<>();
        for (String candidate : candidateIds) {
            AgentInstance instance = id2InstanceMap.get(candidate);
            if (instance != null) {
                terminateIds.add(instance.getId());
            } else {
                unknownInstances.add(candidate);
            }
        }
        if (!unknownInstances.isEmpty()) {
            logger.warn("Instance terminate requested for unknown instances of the instance group {}: {}", instanceGroup.getId(), unknownInstances);
        }
        if (terminateIds.isEmpty()) {
            return Pair.of(Collections.emptySet(), unknownInstances);
        }

        // Check instance group constraint
        int alignedWithGroupMin = Math.min(instanceGroup.getDesired() - instanceGroup.getMin(), terminateIds.size());
        if (alignedWithGroupMin != terminateIds.size()) {
            logger.warn("Terminate request violates instance group {} min size limit. Limiting it to {} from {}",
                    instanceGroup.getId(), alignedWithGroupMin, terminateIds.size());
        }

        // Check autoscale rule constraint
        int alignedWithRuleMin = Math.min(instanceGroup.getDesired() - instanceGroup.getAutoScaleRule().getMin(), alignedWithGroupMin);
        if (alignedWithRuleMin != alignedWithGroupMin) {
            logger.warn("Terminate request violates instance group autoscale rule {} min size limit. Limiting it to {} from {}",
                    instanceGroup.getId(), alignedWithRuleMin, terminateIds.size());
        }

        if (alignedWithRuleMin != terminateIds.size()) {
            logger.warn("Not terminating instances of {} due to min size constraints: {}",
                    instanceGroup.getId(), terminateIds.subList(alignedWithRuleMin, terminateIds.size()));
        }

        return Pair.of(new HashSet<>(terminateIds.subList(0, alignedWithRuleMin)), unknownInstances);
    }
}
