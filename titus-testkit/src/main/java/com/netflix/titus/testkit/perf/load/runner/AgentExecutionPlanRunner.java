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
import java.util.Iterator;
import java.util.Optional;

import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.Tier;
import com.netflix.titus.grpc.protogen.TierUpdate;
import com.netflix.titus.simulator.TitusCloudSimulator;
import com.netflix.titus.simulator.TitusCloudSimulator.AddInstanceGroupRequest;
import com.netflix.titus.testkit.perf.load.ExecutionContext;
import com.netflix.titus.testkit.perf.load.plan.AgentExecutionStep;
import com.netflix.titus.testkit.perf.load.plan.AgentExecutionStep.ChangeLifecycleStateStep;
import com.netflix.titus.testkit.perf.load.plan.AgentExecutionStep.ChangeTierStep;
import com.netflix.titus.testkit.perf.load.plan.AgentExecutionStep.CreatePartitionStep;
import com.netflix.titus.testkit.perf.load.plan.AgentExecutionStep.ResizePartitionStep;
import com.netflix.titus.testkit.perf.load.plan.ExecutionPlan;
import com.netflix.titus.testkit.perf.load.plan.ExecutionStep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.subjects.AsyncSubject;

public class AgentExecutionPlanRunner extends ExecutionPlanRunner {

    private static final Logger logger = LoggerFactory.getLogger(AgentExecutionPlanRunner.class);

    private final Iterator<ExecutionStep> planIterator;
    private final ExecutionContext context;

    private final Scheduler.Worker worker;
    private final AsyncSubject<Void> completedSubject = AsyncSubject.create();
    private volatile Subscription actionSubscription;

    public AgentExecutionPlanRunner(ExecutionPlan jobExecutionPlan,
                                    ExecutionContext context,
                                    Scheduler scheduler) {
        this.planIterator = jobExecutionPlan.newInstance();
        this.context = context;

        this.worker = scheduler.createWorker();
    }

    public void start() {
        runNext();
    }

    public void stop() {
        worker.unsubscribe();
        ObservableExt.safeUnsubscribe(actionSubscription);
        completedSubject.onError(new RuntimeException("Stopped"));
    }

    public Completable awaitJobCompletion() {
        return completedSubject.toCompletable();
    }

    private void runNext() {
        if (!planIterator.hasNext()) {
            logger.info("No more steps; terminating...");
            stop();
            return;
        }

        ExecutionStep step = planIterator.next();
        logger.info("Executing step {}", step);

        Observable<Void> action = toCommonAction(step)
                .orElseGet(() -> toAgentAction(step).orElseThrow(
                        () -> new IllegalStateException("Unknown execution step " + step))
                );

        long startTime = worker.now();
        this.actionSubscription = action.subscribe(
                never -> {
                },
                e -> {
                    logger.warn("Agent execution plan execution failed with an error: {}", e.getMessage());
                    worker.schedule(this::runNext);
                },
                () -> {
                    logger.info("Finished executing agent step {} in {}[ms]", step, worker.now() - startTime);
                    worker.schedule(this::runNext);
                }
        );
    }

    private Optional<Observable<Void>> toAgentAction(ExecutionStep step) {
        switch (step.getName()) {
            case AgentExecutionStep.NAME_CREATE_PARTITION:
                return Optional.of(createPartition((CreatePartitionStep) step));
            case AgentExecutionStep.NAME_RESIZE_PARTITION:
                return Optional.of(resizePartition((ResizePartitionStep) step));
            case AgentExecutionStep.NAME_CHANGE_PARTITION_TIER:
                return Optional.of(changePartitionTier((ChangeTierStep) step));
            case AgentExecutionStep.NAME_CHANGE_PARTITION_LIFECYCLE_STATE:
                return Optional.of(changePartitionLifecycle((ChangeLifecycleStateStep) step));
        }
        return Optional.empty();
    }

    private Observable<Void> createPartition(CreatePartitionStep step) {
        Mono<Void> createAction = context.getSimulatedCloudClient().addInstanceGroup(AddInstanceGroupRequest.newBuilder()
                .setId(step.getPartitionName())
                .setInstanceType(step.getAwsInstanceType().name())
                .setMin(step.getMin())
                .setDesired(step.getDesired())
                .setMax(step.getMax())
                .build()
        );

        Mono<Void> partitionCreationChecker = Flux.interval(Duration.ofMillis(1000))
                .takeUntil(tick -> {
                    Optional<AgentInstanceGroup> instanceGroupOpt = context.getCachedAgentManagementClient().findInstanceGroup(step.getPartitionName());
                    instanceGroupOpt.ifPresent(instanceGroup -> logger.info("New instance group created: {}", instanceGroup));
                    return instanceGroupOpt.isPresent();
                })
                .timeout(Duration.ofSeconds(300))
                .ignoreElements()
                .cast(Void.class)
                .doOnSuccess(value -> logger.info("New partition visible in the local cache: {}", step.getPartitionName()));

        return ReactorExt.toObservable(createAction.concatWith(partitionCreationChecker));
    }

    private Observable<Void> resizePartition(ResizePartitionStep step) {
        Mono<Void> action = context.getSimulatedCloudClient().updateCapacity(TitusCloudSimulator.CapacityUpdateRequest.newBuilder()
                .setInstanceGroupId(step.getPartitionName())
                .setCapacity(TitusCloudSimulator.SimulatedInstanceGroup.Capacity.newBuilder()
                        .setMin(step.getMin())
                        .setDesired(step.getDesired())
                        .setMax(step.getMax())
                        .build()
                )
                .build()
        );
        return ReactorExt.toObservable(action);
    }

    private Observable<Void> changePartitionTier(ChangeTierStep step) {
        Completable action = context.getAgentManagementClient().updateInstanceGroupTier(TierUpdate.newBuilder()
                .setInstanceGroupId(step.getPartition())
                .setTier(Tier.valueOf(step.getTier().name()))
                .build()
        );
        return action.toObservable();
    }

    private Observable<Void> changePartitionLifecycle(ChangeLifecycleStateStep step) {
        Completable action = context.getAgentManagementClient().updateInstanceGroupLifecycle(InstanceGroupLifecycleStateUpdate.newBuilder()
                .setInstanceGroupId(step.getPartition())
                .setLifecycleState(InstanceGroupLifecycleState.valueOf(step.getLifecycleState().name()))
                .setDetail("Change")
                .build()
        );
        return action.toObservable();
    }
}
