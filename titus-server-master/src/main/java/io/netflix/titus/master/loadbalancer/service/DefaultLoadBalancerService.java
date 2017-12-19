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

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import io.netflix.titus.api.loadbalancer.model.sanitizer.LoadBalancerJobValidator;
import io.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.api.service.TitusServiceException;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.guice.annotation.Deactivator;
import io.netflix.titus.common.util.limiter.Limiters;
import io.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.batch.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;

@Singleton
public class DefaultLoadBalancerService implements LoadBalancerService {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerService.class);

    private final TitusRuntime runtime;
    private final LoadBalancerConfiguration configuration;
    private final LoadBalancerStore loadBalancerStore;
    private final LoadBalancerJobValidator validator;

    private final LoadBalancerEngine engine;
    private final Scheduler scheduler;

    private Subscription loadBalancerBatches;

    @Inject
    public DefaultLoadBalancerService(TitusRuntime runtime,
                                      LoadBalancerConfiguration configuration,
                                      LoadBalancerConnector loadBalancerConnector,
                                      LoadBalancerStore loadBalancerStore,
                                      V3JobOperations v3JobOperations,
                                      LoadBalancerJobValidator validator) {
        this(runtime, configuration, loadBalancerConnector, loadBalancerStore, v3JobOperations, new TargetTracking(), validator, Schedulers.computation());
    }

    @VisibleForTesting
    DefaultLoadBalancerService(TitusRuntime runtime,
                               LoadBalancerConfiguration configuration,
                               LoadBalancerConnector loadBalancerConnector,
                               LoadBalancerStore loadBalancerStore,
                               V3JobOperations v3JobOperations,
                               TargetTracking targetTracking,
                               LoadBalancerJobValidator validator,
                               Scheduler scheduler) {
        this.runtime = runtime;
        this.configuration = configuration;
        this.loadBalancerStore = loadBalancerStore;
        this.scheduler = scheduler;
        this.validator = validator;

        final long burst = configuration.getRateLimitBurst();
        final long refillPerSec = configuration.getRateLimitRefillPerSec();
        final TokenBucket connectorTokenBucket = Limiters.createFixedIntervalTokenBucket("loadBalancerConnector",
                burst, burst, refillPerSec, 1, TimeUnit.SECONDS);
        this.engine = new LoadBalancerEngine(configuration, v3JobOperations, loadBalancerStore, targetTracking,
                loadBalancerConnector, connectorTokenBucket, scheduler);

    }

    @Override
    public Observable<String> getJobLoadBalancers(String jobId) {
        return loadBalancerStore.retrieveLoadBalancersForJob(jobId)
                .filter(loadBalancerState -> loadBalancerState.getState() == JobLoadBalancer.State.Associated)
                .map(JobLoadBalancerState::getLoadBalancerId);
    }

    @Override
    public Completable addLoadBalancer(String jobId, String loadBalancerId) {
        try {
            validator.validateJobId(jobId);
        } catch (Exception e) {
            return Completable.error(TitusServiceException.invalidArgument(e.getMessage()));
        }

        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Associated)
                .andThen(engine.add(jobLoadBalancer));
    }

    @Override
    public Completable removeLoadBalancer(String jobId, String loadBalancerId) {
        final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(jobId, loadBalancerId);
        return loadBalancerStore.addOrUpdateLoadBalancer(jobLoadBalancer, JobLoadBalancer.State.Dissociated)
                .andThen(engine.remove(jobLoadBalancer));
    }

    @Activator
    public void activate() {
        if (!configuration.isEngineEnabled()) {
            return; // noop
        }

        loadBalancerBatches = runtime.persistentStream(events())
                .subscribeOn(scheduler)
                .subscribe(
                        batch -> logger.info("Load balancer {} batch completed. Size {}", batch.getIndex(), batch.size()),
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
    }

    @VisibleForTesting
    Observable<Batch<TargetStateBatchable, String>> events() {
        return engine.events();
    }
}
