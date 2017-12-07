/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.loadbalancer.service;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

class LoadBalancerEngine {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerEngine.class);

    private final AssociationsTracking associationsTracking = new AssociationsTracking();
    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingAssociations;
    private final Subject<JobLoadBalancer, JobLoadBalancer> pendingDissociations;
    private final V3JobOperations v3JobOperations;
    private final TargetTracking targetTracking;
    private final Batcher batcher;

    LoadBalancerEngine(LoadBalancerConfiguration configuration, V3JobOperations v3JobOperations,
                       LoadBalancerStore loadBalancerStore, TargetTracking targetTracking,
                       LoadBalancerConnector loadBalancerConnector, Scheduler scheduler) {
        // TODO(fabio): load tracking state from store
        this.v3JobOperations = v3JobOperations;
        this.targetTracking = targetTracking;
        pendingAssociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
        pendingDissociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
        final LoadBalancerConfiguration.Batch batchConfig = configuration.getBatch();
        this.batcher = new Batcher(batchConfig.getTimeoutMs(), batchConfig.getSize(), loadBalancerConnector, targetTracking, scheduler);
    }

    public Completable add(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> {
            associationsTracking.add(jobLoadBalancer);
            pendingAssociations.onNext(jobLoadBalancer);
        });
    }

    public Completable remove(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> {
            associationsTracking.remove(jobLoadBalancer);
            pendingDissociations.onNext(jobLoadBalancer);
        });
    }

    Observable<Batch> events() {
        Observable<TaskUpdateEvent> stateTransitions = v3JobOperations.observeJobs()
                .filter(TaskUpdateEvent.class::isInstance)
                .cast(TaskUpdateEvent.class)
                .filter(LoadBalancerEngine::isStateTransition);

        // TODO(fabio): rate limit changes from task events and API calls to allow space for reconciliation?
        final Observable<LoadBalancerTarget> toRegister = Observable.merge(
                registerFromAssociations(pendingAssociations),
                registerFromEvents(stateTransitions)
        );
        final Observable<LoadBalancerTarget> toDeregister = Observable.merge(
                deregisterFromDissociations(pendingDissociations),
                deregisterFromEvents(stateTransitions)
        );

        return batcher.events(toRegister, toDeregister);
    }

    public void shutdown() {
        this.pendingAssociations.onCompleted();
        this.pendingDissociations.onCompleted();
    }

    private Observable<LoadBalancerTarget> registerFromEvents(Observable<TaskUpdateEvent> events) {
        Observable<Task> tasks = events.map(TaskUpdateEvent::getCurrentTask)
                .filter(LoadBalancerEngine::isStartedWithIp);
        return targetsForTrackedTasks(tasks);
    }

    private Observable<LoadBalancerTarget> deregisterFromEvents(Observable<TaskUpdateEvent> events) {
        Observable<Task> tasks = events.map(TaskUpdateEvent::getCurrentTask)
                .filter(LoadBalancerEngine::isTerminalWithIp);
        return targetsForTrackedTasks(tasks);
    }

    private Observable<LoadBalancerTarget> targetsForTrackedTasks(Observable<Task> tasks) {
        return tasks.doOnNext(task -> logger.debug("Checking if task is in job being tracked: {}", task))
                .filter(this::isTracked)
                .map(task -> Pair.of(task, associationsTracking.get(task.getJobId())))
                .doOnNext(pair -> logger.info("Task update in job being tracked, enqueuing {} load balancer updates: {}",
                        pair.getRight().size(), pair.getLeft()))
                .flatMap(pair -> Observable.from(
                        pair.getRight().stream().map(association -> new LoadBalancerTarget(
                                association,
                                pair.getLeft().getId(),
                                pair.getLeft().getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)
                        )).collect(Collectors.toList()))
                );
    }

    private Observable<LoadBalancerTarget> registerFromAssociations(Observable<JobLoadBalancer> pendingAssociations) {
        return pendingAssociations
                .filter(jobLoadBalancer -> v3JobOperations.getJob(jobLoadBalancer.getJobId()).isPresent())
                .flatMap(jobLoadBalancer -> Observable.from(targetsForJob(jobLoadBalancer))
                        .doOnError(e -> logger.error("Error loading targets for jobId " + jobLoadBalancer.getJobId(), e))
                        .onErrorResumeNext(Observable.empty()))
                .doOnError(e -> logger.error("Error fetching targets to register", e))
                .retry();
    }

    private Observable<LoadBalancerTarget> deregisterFromDissociations(Observable<JobLoadBalancer> pendingDissociations) {
        return pendingDissociations
                .flatMap(
                        // fetch everything, including deregistered, so they are retried
                        jobLoadBalancer -> targetTracking.retrieveTargets(jobLoadBalancer)
                                .map(targetState -> new LoadBalancerTarget(
                                        jobLoadBalancer, targetState.getLoadBalancerTarget().getTaskId(), targetState.getLoadBalancerTarget().getIpAddress()
                                )))
                .doOnError(e -> logger.error("Error fetching targets to deregister", e))
                .retry();
    }

    /**
     * Valid targets are tasks in the Started state that have ip addresses associated to them.
     */
    private List<LoadBalancerTarget> targetsForJob(JobLoadBalancer jobLoadBalancer) {
        return v3JobOperations.getTasks(jobLoadBalancer.getJobId()).stream()
                .filter(LoadBalancerEngine::isStartedWithIp)
                .map(task -> new LoadBalancerTarget(
                        jobLoadBalancer,
                        task.getId(),
                        task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)
                ))
                .collect(Collectors.toList());
    }

    private boolean isTracked(Task task) {
        return !associationsTracking.get(task.getJobId()).isEmpty();
    }

    private static boolean isStateTransition(TaskUpdateEvent event) {
        final Task currentTask = event.getCurrentTask();
        final Optional<Task> previousTask = event.getPreviousTask();
        boolean identical = previousTask.map(previous -> previous == currentTask).orElse(false);
        return !identical && previousTask
                .map(previous -> !previous.getStatus().getState().equals(currentTask.getStatus().getState()))
                .orElse(false);
    }

    private static boolean isStartedWithIp(Task task) {
        return hasIpAndStateMatches(task, TaskState.Started::equals);
    }

    private static boolean isTerminalWithIp(Task task) {
        return hasIpAndStateMatches(task, state -> {
            switch (task.getStatus().getState()) {
                case KillInitiated:
                case Finished:
                case Disconnected:
                    return true;
                default:
                    return false;
            }
        });
    }

    private static boolean hasIpAndStateMatches(Task task, Function<TaskState, Boolean> predicate) {
        final TaskState state = task.getStatus().getState();
        if (!predicate.apply(state)) {
            return false;
        }
        final boolean hasIp = task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
        if (!hasIp) {
            logger.warn("Task {} has state {} but no ipAddress associated", task.getId(), state);
        }
        return hasIp;

    }
}
