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

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.LoadBalancerConnector;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State;
import io.netflix.titus.api.loadbalancer.model.TargetState;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.batch.Priority;
import rx.Observable;
import rx.Scheduler;

public class DefaultLoadBalancerReconciler implements LoadBalancerReconciler {
    private static final String UNKNOWN_JOB = "UNKNOWN-JOB";
    private static final String UNKNOWN_TASK = "UNKNOWN-TASK";

    private final LoadBalancerStore store;
    private final LoadBalancerConnector connector;
    private final JobOperations jobOperations;
    private final long delayMs;
    private final Scheduler scheduler;

    DefaultLoadBalancerReconciler(LoadBalancerConfiguration configuration,
                                  LoadBalancerStore store,
                                  LoadBalancerConnector connector,
                                  JobOperations jobOperations,
                                  Scheduler scheduler) {
        this.store = store;
        this.connector = connector;
        this.jobOperations = jobOperations;
        this.delayMs = configuration.getReconciliationDelayMs();
        this.scheduler = scheduler;
    }

    @Override
    public Observable<TargetStateBatchable> events() {
        Observable<TargetStateBatchable> updatesForAll = Observable.fromCallable(this::byLoadBalancer)
                .flatMapIterable(Map::entrySet, 1)
                // TODO(fabio): rate limit calls to reconcile (and to the connector)
                .flatMap(entry -> reconcile(entry.getKey(), entry.getValue()), 1);

        // TODO(fabio): timeout for each run (subscription)

        return ObservableExt.periodicGenerator(updatesForAll, delayMs, delayMs, TimeUnit.MILLISECONDS, scheduler)
                .flatMap(Observable::from, 1);
    }

    private Observable<TargetStateBatchable> reconcile(String loadBalancerId, List<JobLoadBalancerState> associations) {
        Set<LoadBalancerTarget> shouldBeRegistered = associations.stream()
                .filter(JobLoadBalancerState::isStateAssociated)
                .flatMap(association -> jobOperations.targetsForJob(association.getJobLoadBalancer()).stream())
                .collect(Collectors.toSet());
        final Set<String> shouldBeRegisteredIps = shouldBeRegistered.stream()
                .map(LoadBalancerTarget::getIpAddress)
                .collect(Collectors.toSet());

        final Instant now = now();
        return connector.getRegisteredIps(loadBalancerId).flatMapObservable(registeredIps -> {
            // FIXME(fabio): do not register something that is being deregistered
            Set<LoadBalancerTarget> toRegister = shouldBeRegistered.stream()
                    .filter(target -> !registeredIps.contains(target.getIpAddress()))
                    .collect(Collectors.toSet());

            // FIXME(fabio): do not deregister something that is being registered
            Set<LoadBalancerTarget> toDeregister = CollectionsExt.copyAndRemove(registeredIps, shouldBeRegisteredIps).stream()
                    .map(ip -> updateForUnknownTask(loadBalancerId, ip))
                    .collect(Collectors.toSet());

            return Observable.from(CollectionsExt.merge(
                    withState(now, toRegister, State.Registered),
                    withState(now, toDeregister, State.Deregistered)
            ));
        });
    }

    /**
     * Hack until we start keeping track of everything that was registered on a load balancer. For now, we generate
     * <tt>State.Deregistered</tt> updates for dummy tasks since we only care about the <tt>loadBalancerId</tt> and
     * the <tt>ipAddress</tt> for deregistrations.
     */
    private LoadBalancerTarget updateForUnknownTask(String loadBalancerId, String ip) {
        return new LoadBalancerTarget(new JobLoadBalancer(UNKNOWN_JOB, loadBalancerId), UNKNOWN_TASK, ip);
    }

    private List<TargetStateBatchable> withState(Instant instant, Collection<LoadBalancerTarget> targets, State state) {
        return targets.stream()
                .map(target -> new TargetStateBatchable(Priority.Low, instant, new TargetState(target, state)))
                .collect(Collectors.toList());
    }

    private Map<String, List<JobLoadBalancerState>> byLoadBalancer() {
        return store.getAssociations().stream()
                .collect(Collectors.groupingBy(JobLoadBalancerState::getLoadBalancerId));
    }

    private Instant now() {
        return Instant.ofEpochMilli(scheduler.now());
    }
}
