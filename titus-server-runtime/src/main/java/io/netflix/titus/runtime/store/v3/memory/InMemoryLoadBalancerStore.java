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

package io.netflix.titus.runtime.store.v3.memory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import rx.Completable;
import rx.Observable;

public class InMemoryLoadBalancerStore implements LoadBalancerStore {

    private final ConcurrentMap<JobLoadBalancer, Set<LoadBalancerTarget>> jobLoadBalancers;

    public InMemoryLoadBalancerStore() {
        jobLoadBalancers = new ConcurrentHashMap<>();
    }

    @Override
    public Observable<String> retrieveLoadBalancersForJob(String jobId) {
        return Observable.from(jobLoadBalancers.keySet())
                .filter(jobLoadBalancer -> jobLoadBalancer.getJobId().equals(jobId))
                .map(JobLoadBalancer::getLoadBalancerId)
                .distinct();
    }

    @Override
    public Completable addLoadBalancer(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> jobLoadBalancers.putIfAbsent(jobLoadBalancer, ConcurrentHashMap.newKeySet()));
    }

    @Override
    public Completable removeLoadBalancer(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> jobLoadBalancers.remove(jobLoadBalancer));
    }

    @Override
    public Observable<LoadBalancerTarget> retrieveTargets(JobLoadBalancer jobLoadBalancer) {
        final Set<LoadBalancerTarget> targets = jobLoadBalancers.get(jobLoadBalancer);
        if (targets == null) {
            return Observable.empty();
        }
        return Observable.from(targets);
    }

    @Override
    public Completable updateTargets(Collection<LoadBalancerTarget> targets) {
        if (CollectionsExt.isNullOrEmpty(targets)) {
            return Completable.complete();
        }

        final Map<JobLoadBalancer, List<LoadBalancerTarget>> grouped = targets.stream()
                .collect(Collectors.groupingBy(LoadBalancerTarget::getJobLoadBalancer));
        return Observable.from(grouped.keySet()).flatMap(
                jobLoadBalancer -> Observable.from(grouped.get(jobLoadBalancer))
                        .doOnNext(target -> jobLoadBalancers.get(jobLoadBalancer).add(target))
        ).toCompletable();
    }
}
