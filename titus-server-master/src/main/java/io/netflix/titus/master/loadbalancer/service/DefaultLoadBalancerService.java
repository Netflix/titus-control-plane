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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.Deactivator;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerService.class);

    private final LoadBalancerConfiguration configuration;
    private final LoadBalancerClient loadBalancerClient;
    private final LoadBalancerStore loadBalancerStore;
    private final V3JobOperations v3JobOperations;

    private PublishSubject<JobLoadBalancer> pendingAssociations;
    private PublishSubject<JobLoadBalancer> pendingDissociations;
    private Subscription loadBalancerBatches;

    @Inject
    public DefaultLoadBalancerService(LoadBalancerConfiguration configuration,
                                      LoadBalancerClient loadBalancerClient,
                                      LoadBalancerStore loadBalancerStore,
                                      V3JobOperations v3JobOperations) {
        this.configuration = configuration;
        this.loadBalancerClient = loadBalancerClient;
        this.loadBalancerStore = loadBalancerStore;
        this.v3JobOperations = v3JobOperations;
    }

    private static boolean hasIp(Task task) {
        final boolean hasIp = task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
        if (!hasIp) {
            logger.warn("Task {} has state {} but no ipAddress associated", task.getId(), task.getStatus().getState());
        }
        return hasIp;
    }

    @Activator
    public void startReconciliation() {
        pendingAssociations = PublishSubject.create();
        pendingDissociations = PublishSubject.create();

        final Observable<LoadBalancerTarget> toRegister = pendingAssociations.flatMap(
                jobLoadBalancer -> {
                    final String jobId = jobLoadBalancer.getJobId();
                    if (!v3JobOperations.getJob(jobId).isPresent()) {
                        // job is gone, nothing do to
                        return Observable.empty();
                    }
                    final LoadBalancerTarget.State desiredState = LoadBalancerTarget.State.Registered;
                    return Observable.merge(v3JobOperations.getTasks(jobId).stream()
                            .filter(task -> task.getStatus().getState() == TaskState.Started)
                            .filter(DefaultLoadBalancerService::hasIp)
                            .map(task -> {
                                final String ipAddress = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_CONTAINER_IP);
                                return Observable.just(new LoadBalancerTarget(jobLoadBalancer, task.getId(), ipAddress, desiredState));
                            }).collect(Collectors.toList()));
                }
        );

        final Observable<LoadBalancerTarget> toDeregister = pendingDissociations.flatMap(
                jobLoadBalancer -> loadBalancerStore.retrieveTargets(jobLoadBalancer).map(
                        target -> {
                            final LoadBalancerTarget.State desiredState = LoadBalancerTarget.State.Deregistered;
                            return new LoadBalancerTarget(jobLoadBalancer, target.getTaskId(), target.getIpAddress(), desiredState);
                        }
                )
        );

        final Observable<Void> loadBalancerServiceCalls = Observable.merge(toRegister, toDeregister)
                .buffer(configuration.getBatch().getTimeoutMs(), TimeUnit.MILLISECONDS,
                        configuration.getBatch().getSize())
                .map(CollectionsExt::distinctKeepLast)
                .flatMap(batch -> {
                    Map<LoadBalancerTarget.State, List<LoadBalancerTarget>> groupedBy =
                            batch.stream().collect(Collectors.groupingBy(LoadBalancerTarget::getState));
                    final List<LoadBalancerTarget> registerList = groupedBy.get(LoadBalancerTarget.State.Registered);
                    final List<LoadBalancerTarget> deregisterList = groupedBy.get(LoadBalancerTarget.State.Deregistered);
                    return Completable.mergeDelayError(
                            loadBalancerClient.registerAll(registerList).andThen(loadBalancerStore.updateTargets(registerList)),
                            loadBalancerClient.deregisterAll(deregisterList).andThen(loadBalancerStore.updateTargets(deregisterList))
                    ).toObservable();
                });

        loadBalancerBatches = loadBalancerServiceCalls.subscribe(
                ignored -> logger.info("Load balancer batch completed"),
                e -> logger.error("Error while processing load balancer batch", e),
                () -> logger.info("Load balancer batch stream closed")
        );

        // TODO(fabio): reconciliation
        // TODO(fabio): watch task and job update streams
        // TODO(fabio): garbage collect removed jobs and loadbalancers
        // TODO(fabio): garbage collect removed tasks
        // TODO(fabio): integrate with the V2 engine
    }

    @Deactivator
    public void stopReconciliation() {
        ObservableExt.safeUnsubscribe(loadBalancerBatches);

        this.pendingAssociations.onCompleted();
        this.pendingDissociations.onCompleted();
    }

    @Override
    public Observable<String> getJobLoadBalancers(String jobId) {
        return loadBalancerStore.retrieveLoadBalancersForJob(jobId);
    }

    @Override
    public Completable addLoadBalancer(String jobId, String loadBalancerId) {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addLoadBalancer(jobLoadBalancer)
                .andThen(Completable.fromAction(
                        () -> pendingAssociations.onNext(jobLoadBalancer)
                ));
    }

    @Override
    public Completable removeLoadBalancer(String jobId, String loadBalancerId) {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.removeLoadBalancer(jobLoadBalancer)
                .andThen(Completable.fromAction(
                        () -> pendingDissociations.onNext(jobLoadBalancer)
                ));
    }
}
