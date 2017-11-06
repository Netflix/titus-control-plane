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
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.jobmanager.model.event.TaskUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.Deactivator;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerService.class);

    private final TitusRuntime runtime;
    private final LoadBalancerStore loadBalancerStore;
    private final V3JobOperations v3JobOperations;

    private final Scheduler scheduler;
    private final Batcher batcher;
    private final Tracking tracking = new Tracking();

    private Subject<JobLoadBalancer, JobLoadBalancer> pendingAssociations;
    private Subject<JobLoadBalancer, JobLoadBalancer> pendingDissociations;
    private Subscription loadBalancerBatches;

    @Inject
    public DefaultLoadBalancerService(TitusRuntime runtime,
                                      LoadBalancerConfiguration configuration,
                                      LoadBalancerClient loadBalancerClient,
                                      LoadBalancerStore loadBalancerStore,
                                      V3JobOperations v3JobOperations) {
        this(runtime, configuration, loadBalancerClient, loadBalancerStore, v3JobOperations, Schedulers.computation());
    }

    public DefaultLoadBalancerService(TitusRuntime runtime,
                                      LoadBalancerConfiguration configuration,
                                      LoadBalancerClient loadBalancerClient,
                                      LoadBalancerStore loadBalancerStore,
                                      V3JobOperations v3JobOperations,
                                      Scheduler scheduler) {
        this.runtime = runtime;
        this.loadBalancerStore = loadBalancerStore;
        this.v3JobOperations = v3JobOperations;
        this.scheduler = scheduler;
        this.batcher = new Batcher(configuration.getBatch().getTimeoutMs(), configuration.getBatch().getSize(),
                loadBalancerClient, loadBalancerStore, scheduler);
    }

    @Override
    public Observable<String> getJobLoadBalancers(String jobId) {
        return loadBalancerStore.retrieveLoadBalancersForJob(jobId)
                .filter(pair -> pair.getRight() == JobLoadBalancer.State.Associated)
                .map(Pair::getLeft);
    }

    @Override
    public Completable addLoadBalancer(String jobId, String loadBalancerId) {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Associated)
                .andThen(Completable.fromAction(() -> tracking.add(jobLoadBalancer)))
                .andThen(Completable.fromAction(() -> pendingAssociations.onNext(jobLoadBalancer)));
    }

    @Override
    public Completable removeLoadBalancer(String jobId, String loadBalancerId) {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Dissociated)
                .andThen(Completable.fromAction(() -> tracking.remove(jobLoadBalancer)))
                .andThen(Completable.fromAction(() -> pendingDissociations.onNext(jobLoadBalancer)));
    }

    @Activator
    public void activate() {
        // TODO(fabio): load tracking state from store

        loadBalancerBatches = runtime.persistentStream(events())
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .subscribe(
                        batch -> logger.info("Load balancer batch completed. Registered {}, deregistered {}",
                                batch.getStateRegister().size(), batch.getStateDeregister().size()),
                        e -> logger.error("Error while processing load balancer batch", e),
                        () -> logger.info("Load balancer batch stream closed")
                );

        // TODO(fabio): reconciliation
        // TODO(fabio): watch job updates stream for garbage collection
        // TODO(fabio): garbage collect removed jobs and loadbalancers
        // TODO(fabio): integrate with the V2 engine
    }

    @Deactivator
    public void deactivate() {
        ObservableExt.safeUnsubscribe(loadBalancerBatches);

        this.pendingAssociations.onCompleted();
        this.pendingDissociations.onCompleted();
    }


    @VisibleForTesting
    Observable<Batch> events() {
        pendingAssociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
        pendingDissociations = PublishSubject.<JobLoadBalancer>create().toSerialized();

        Observable<TaskUpdateEvent> stateTransitions = v3JobOperations.observeJobs()
                .filter(TaskUpdateEvent.class::isInstance)
                .cast(TaskUpdateEvent.class)
                .filter(event -> event.getModel() == ModelUpdateAction.Model.Reference && event.getTask().isPresent())
                .filter(StreamHelpers::isStateTransition);

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

    private Observable<LoadBalancerTarget> registerFromEvents(Observable<TaskUpdateEvent> events) {
        // Optional.empty() tasks have been already filtered out
        //noinspection ConstantConditions
        Observable<Task> tasks = events.map(event -> event.getTask().get())
                .filter(StreamHelpers::isStartedWithIp);
        return targetsForTrackedTasks(tasks);
    }

    private Observable<LoadBalancerTarget> deregisterFromEvents(Observable<TaskUpdateEvent> events) {
        // Optional.empty() tasks have been already filtered out
        //noinspection ConstantConditions
        Observable<Task> tasks = events.map(event -> event.getTask().get())
                .filter(StreamHelpers::isTerminalWithIp);
        return targetsForTrackedTasks(tasks);
    }

    private Observable<LoadBalancerTarget> targetsForTrackedTasks(Observable<Task> tasks) {
        return tasks.doOnNext(task -> logger.debug("Checking if task is in job being tracked: {}", task))
                .filter(this::isTracked)
                .map(task -> Pair.of(task, tracking.get(task.getJobId())))
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
                        jobLoadBalancer -> loadBalancerStore.retrieveTargets(jobLoadBalancer)
                                .map(pair -> new LoadBalancerTarget(
                                        jobLoadBalancer, pair.getLeft().getTaskId(), pair.getLeft().getIpAddress()
                                )))
                .doOnError(e -> logger.error("Error fetching targets to deregister", e))
                .retry();
    }

    /**
     * Valid targets are tasks in the Started state that have ip addresses associated to them.
     */
    private List<LoadBalancerTarget> targetsForJob(JobLoadBalancer jobLoadBalancer) {
        return v3JobOperations.getTasks(jobLoadBalancer.getJobId()).stream()
                .filter(StreamHelpers::isStartedWithIp)
                .map(task -> new LoadBalancerTarget(
                        jobLoadBalancer,
                        task.getId(),
                        task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)
                ))
                .collect(Collectors.toList());
    }

    private boolean isTracked(Task task) {
        return !tracking.get(task.getJobId()).isEmpty();
    }

    private interface StreamHelpers {

        static boolean isStateTransition(TaskUpdateEvent event) {
            final Task currentTask = event.getTask().get();
            final Optional<Task> previousTask = event.getPreviousTaskVersion();
            boolean identical = previousTask.map(previous -> previous == currentTask).orElse(false);
            return !identical && previousTask
                    .map(previous -> !previous.getStatus().getState().equals(currentTask.getStatus().getState()))
                    .orElse(false);
        }

        static boolean isStartedWithIp(Task task) {
            return hasIpAndStateMatches(task, TaskState.Started::equals);
        }

        static boolean isTerminalWithIp(Task task) {
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

        static boolean hasIpAndStateMatches(Task task, Function<TaskState, Boolean> predicate) {
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
}
