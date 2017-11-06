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
import java.util.function.Function;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.LoadBalancerClient;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Scheduler;

/**
 * Buffer and batch LoadBalancer operations to reduce the number of remote calls
 */
class Batcher {
    private static Logger logger = LoggerFactory.getLogger(Batcher.class);

    private final long timeoutMs;
    private final int batchSize;
    private final LoadBalancerClient loadBalancerClient;
    private final LoadBalancerStore loadBalancerStore;
    private final Scheduler scheduler;

    Batcher(long timeoutMs, int batchSize, LoadBalancerClient loadBalancerClient, LoadBalancerStore loadBalancerStore, Scheduler scheduler) {
        this.timeoutMs = timeoutMs;
        this.batchSize = batchSize;
        this.loadBalancerClient = loadBalancerClient;
        this.loadBalancerStore = loadBalancerStore;
        this.scheduler = scheduler;
    }

    Observable<Batch> events(Observable<LoadBalancerTarget> targetsToRegister, Observable<LoadBalancerTarget> targetsToDeregister) {
        final Observable<Pair<LoadBalancerTarget, LoadBalancerTarget.State>> mergedWithState = Observable.merge(
                targetsToRegister.map(target -> Pair.of(target, LoadBalancerTarget.State.Registered)),
                targetsToDeregister.map(target -> Pair.of(target, LoadBalancerTarget.State.Deregistered))
        );
        return mergedWithState
                .doOnNext(pair -> logger.debug("Buffering load balancer target {} -> {}", pair.getLeft(), pair.getRight()))
                .buffer(timeoutMs, TimeUnit.MILLISECONDS, batchSize, scheduler)
                .doOnNext(list -> logger.debug("Processing batch operation of size {}", list.size()))
                .map(targets -> targets.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight)))
                .flatMap(this::processBatch)
                .doOnNext(batch -> logger.info("Processed load balancer batch: registered {}, deregistered {}",
                        batch.getStateRegister().size(), batch.getStateDeregister().size()))
                .doOnError(e -> logger.error("Error batching load balancer calls", e))
                .retry();
    }

    /**
     * rxJava 1.x doesn't have the Maybe type. This could also return a Single<Optional<Batch>>, but an
     * Observable that emits a single item (or none in case of errors) is simpler
     *
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

}
