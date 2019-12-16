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

package com.netflix.titus.runtime.store.v3.memory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget.State;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import com.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import com.netflix.titus.runtime.loadbalancer.LoadBalancerCursors;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

/**
 * Operations are not being indexed yet for simplicity.
 */
public class InMemoryLoadBalancerStore implements LoadBalancerStore {
    private final ConcurrentMap<JobLoadBalancer, JobLoadBalancer.State> associations = new ConcurrentHashMap<>();
    private final ConcurrentMap<LoadBalancerTarget, State> targets = new ConcurrentHashMap<>();

    @Override
    public Observable<JobLoadBalancer> getAssociatedLoadBalancersForJob(String jobId) {
        return Observable.defer(() -> Observable.from(getAssociatedLoadBalancersSetForJob(jobId)));
    }

    // Note: This implementation is not optimized for constant time lookup and should only
    // be used for non-performance critical scenarios, like testing.
    @Override
    public Set<JobLoadBalancer> getAssociatedLoadBalancersSetForJob(String jobId) {
        return associations.entrySet().stream()
                .filter(pair -> pair.getKey().getJobId().equals(jobId) && (pair.getValue() == JobLoadBalancer.State.ASSOCIATED))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Override
    public Completable addOrUpdateLoadBalancer(JobLoadBalancer jobLoadBalancer, JobLoadBalancer.State state) {
        return Completable.fromAction(() -> associations.put(jobLoadBalancer, state));
    }

    @Override
    public Completable removeLoadBalancer(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> associations.remove(jobLoadBalancer));
    }

    @Override
    public int getNumLoadBalancersForJob(String jobId) {
        int loadBalancerCount = 0;
        for (Map.Entry<JobLoadBalancer, JobLoadBalancer.State> entry : associations.entrySet()) {
            if (entry.getKey().getJobId().equals(jobId)) {
                loadBalancerCount++;
            }
        }
        return loadBalancerCount;
    }

    @Override
    public List<JobLoadBalancerState> getAssociations() {
        return associations.entrySet().stream()
                .map(JobLoadBalancerState::from)
                .collect(Collectors.toList());
    }

    @Override
    public List<JobLoadBalancer> getAssociationsPage(int offset, int limit) {
        return associations.keySet().stream()
                .sorted(LoadBalancerCursors.loadBalancerComparator())
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());
    }

    @Override
    public Mono<Void> addOrUpdateTargets(Collection<LoadBalancerTargetState> toAdd) {
        return Mono.fromRunnable(() -> toAdd.forEach(t -> targets.put(t.getLoadBalancerTarget(), t.getState())));
    }

    @Override
    public Mono<Void> removeDeregisteredTargets(Collection<LoadBalancerTarget> toRemove) {
        return Mono.fromRunnable(() -> toRemove.forEach(target -> targets.remove(target, State.DEREGISTERED)));
    }

    @Override
    public Flux<LoadBalancerTargetState> getLoadBalancerTargets(String loadBalancerId) {
        return Flux.fromStream(
                targets.entrySet().stream()
                        .filter(entry -> entry.getKey().getLoadBalancerId().equals(loadBalancerId))
                        .map(LoadBalancerTargetState::from)
        );
    }
}
