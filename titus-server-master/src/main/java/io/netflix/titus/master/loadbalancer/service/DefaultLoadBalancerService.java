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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
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

    private final LoadBalancerConfiguration configuration;
    private final LoadBalancerClient loadBalancerClient;
    private final LoadBalancerStore loadBalancerStore;
    private final V3JobOperations v3JobOperations;

    private final Scheduler scheduler;

    private final ConcurrentMap<JobLoadBalancer, JobLoadBalancer.State> tracking = new ConcurrentHashMap<>();

    private Subject<JobLoadBalancer, JobLoadBalancer> pendingAssociations;
    private Subject<JobLoadBalancer, JobLoadBalancer> pendingDissociations;
    private Subscription loadBalancerBatches;

    @Inject
    public DefaultLoadBalancerService(LoadBalancerConfiguration configuration,
                                      LoadBalancerClient loadBalancerClient,
                                      LoadBalancerStore loadBalancerStore,
                                      V3JobOperations v3JobOperations) {
        this(configuration, loadBalancerClient, loadBalancerStore, v3JobOperations, Schedulers.computation());
    }

    public DefaultLoadBalancerService(LoadBalancerConfiguration configuration,
                                      LoadBalancerClient loadBalancerClient,
                                      LoadBalancerStore loadBalancerStore,
                                      V3JobOperations v3JobOperations,
                                      Scheduler scheduler) {
        this.configuration = configuration;
        this.loadBalancerClient = loadBalancerClient;
        this.loadBalancerStore = loadBalancerStore;
        this.v3JobOperations = v3JobOperations;
        this.scheduler = scheduler;
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
                .andThen(Completable.fromAction(() -> pendingAssociations.onNext(jobLoadBalancer)));
    }

    @Override
    public Completable removeLoadBalancer(String jobId, String loadBalancerId) {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Dissociated)
                .andThen(Completable.fromAction(() -> pendingDissociations.onNext(jobLoadBalancer)));
    }

    @VisibleForTesting
    Observable<Batch> buildStream() {
        pendingAssociations = PublishSubject.<JobLoadBalancer>create().toSerialized();
        pendingDissociations = PublishSubject.<JobLoadBalancer>create().toSerialized();

        final Observable<LoadBalancerTarget> toRegister = targetsToRegister(pendingAssociations);
        final Observable<LoadBalancerTarget> toDeregister = targetsToDeregister(pendingDissociations);
        return batchLoadBalancerChanges(toRegister, toDeregister);
    }

    @Activator
    public void activate() {
        loadBalancerBatches = buildStream()
                .observeOn(scheduler)
                .subscribeOn(scheduler)
                .subscribe(
                        batch -> logger.info("Load balancer batch completed. Registered {}, deregistered {}",
                                batch.getStateRegister().size(), batch.getStateDeregister().size()),
                        e -> logger.error("Error while processing load balancer batch", e),
                        () -> logger.info("Load balancer batch stream closed")
                );

        // TODO(fabio): reconciliation
        // TODO(fabio): watch task and job update streams
        // TODO(fabio): garbage collect removed jobs and loadbalancers
        // TODO(fabio): integrate with the V2 engine
    }

    @Deactivator
    public void deactivate() {
        ObservableExt.safeUnsubscribe(loadBalancerBatches);

        this.pendingAssociations.onCompleted();
        this.pendingDissociations.onCompleted();
    }

    private Observable<Batch> batchLoadBalancerChanges(Observable<LoadBalancerTarget> targetsToRegister, Observable<LoadBalancerTarget> targetsToDeregister) {
        final Observable<Pair<LoadBalancerTarget, LoadBalancerTarget.State>> mergedWithState = Observable.merge(
                targetsToRegister.map(target -> Pair.of(target, LoadBalancerTarget.State.Registered)),
                targetsToDeregister.map(target -> Pair.of(target, LoadBalancerTarget.State.Deregistered))
        );
        return mergedWithState
                .doOnNext(pair -> logger.debug("Buffering load balancer target {} -> {}", pair.getLeft(), pair.getRight()))
                .buffer(configuration.getBatch().getTimeoutMs(), TimeUnit.MILLISECONDS, configuration.getBatch().getSize(), scheduler)
                .doOnNext(list -> logger.debug("Processing batch operation of size {}", list.size()))
                .map(targets -> targets.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight)))
                .flatMap(this::processBatch)
                .doOnNext(batch -> logger.info("Processed load balancer batch: registered {}, deregistered {}",
                        batch.getStateRegister().size(), batch.getStateDeregister().size()))
                .doOnError(e -> logger.error("Error batching load balancer calls", e))
                .retry();
    }

    /**
     * rxJava 1.x doesn't have the Maybe type. This could also return a Single<Either<Batch, Throwable>>, but an
     * Observable that emits a single item (or none in case of errors) is simpler
     *
     * @param targets
     * @return an Observable that emits either a single batch, or none in case of errors
     */
    private Observable<Batch> processBatch(Map<LoadBalancerTarget, LoadBalancerTarget.State> targets) {
        final Batch grouped = new Batch(targets);
        final List<LoadBalancerTarget> registerList = grouped.getStateRegister();
        final List<LoadBalancerTarget> deregisterList = grouped.getStateDeregister();
        final Completable updateRegistered = loadBalancerStore.updateTargets(registerList.stream()
                .collect(Collectors.toMap(Function.identity(), ignored -> LoadBalancerTarget.State.Registered)));
        final Completable merged = Completable.mergeDelayError(
                loadBalancerClient.registerAll(registerList).andThen(updateRegistered),
                loadBalancerClient.deregisterAll(deregisterList).andThen(loadBalancerStore.removeTargets(deregisterList))
        );
        return merged.andThen(Observable.just(grouped))
                .doOnError(e -> logger.error("Error processing batch " + grouped, e))
                .onErrorResumeNext(Observable.empty());
    }

    private Observable<LoadBalancerTarget> targetsToDeregister(Observable<JobLoadBalancer> pendingDissociations) {
        return pendingDissociations.flatMap(
                // fetch everything, including deregistered, so they are retried
                jobLoadBalancer -> loadBalancerStore.retrieveTargets(jobLoadBalancer)
                        .map(pair -> new LoadBalancerTarget(
                                jobLoadBalancer, pair.getLeft().getTaskId(), pair.getLeft().getIpAddress()
                        )))
                .doOnError(e -> logger.error("Error fetching targets to deregister", e))
                .retry();
    }

    private Observable<LoadBalancerTarget> targetsToRegister(Observable<JobLoadBalancer> pendingAssociations) {
        return pendingAssociations
                .filter(jobLoadBalancer -> v3JobOperations.getJob(jobLoadBalancer.getJobId()).isPresent())
                .flatMap(jobLoadBalancer -> Observable.from(targetsForJob(jobLoadBalancer))
                        .doOnError(e -> logger.error("Error loading targets for jobId " + jobLoadBalancer.getJobId(), e))
                        .onErrorResumeNext(Observable.empty()))
                .doOnError(e -> logger.error("Error fetching targets to register", e))
                .retry();
    }

    /**
     * Valid targets are tasks in the Started state that have ip addresses associated to them.
     */
    private List<LoadBalancerTarget> targetsForJob(JobLoadBalancer jobLoadBalancer) {
        return v3JobOperations.getTasks(jobLoadBalancer.getJobId()).stream()
                .filter(task -> task.getStatus().getState() == TaskState.Started)
                .filter(DefaultLoadBalancerService::hasIp)
                .map(task -> new LoadBalancerTarget(
                        jobLoadBalancer,
                        task.getId(),
                        task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP)
                ))
                .collect(Collectors.toList());
    }

    private static boolean hasIp(Task task) {
        final boolean hasIp = task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
        if (!hasIp) {
            logger.warn("Task {} has state {} but no ipAddress associated", task.getId(), task.getStatus().getState());
        }
        return hasIp;
    }

    @VisibleForTesting
    static class Batch {
        private final Map<LoadBalancerTarget.State, List<LoadBalancerTarget>> groupedBy;

        public Batch(Map<LoadBalancerTarget, LoadBalancerTarget.State> batch) {
            groupedBy = batch.entrySet().stream().collect(
                    Collectors.groupingBy(
                            Map.Entry::getValue,
                            Collectors.mapping(Map.Entry::getKey, Collectors.toList())));
        }

        private List<LoadBalancerTarget> getStateRegister() {
            return groupedBy.getOrDefault(LoadBalancerTarget.State.Registered, Collections.emptyList());
        }

        private List<LoadBalancerTarget> getStateDeregister() {
            return groupedBy.getOrDefault(LoadBalancerTarget.State.Deregistered, Collections.emptyList());
        }

        @Override
        public String toString() {
            return "Batch{" +
                    "groupedBy=" + groupedBy +
                    '}';
        }
    }
}
