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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.store.LoadBalancerStore;
import io.netflix.titus.common.util.CollectionsExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

/**
 * Operations are not being indexed yet for simplicity.
 */
public class InMemoryLoadBalancerStore implements LoadBalancerStore {
    private static Logger logger = LoggerFactory.getLogger(InMemoryLoadBalancerStore.class);

    private final Set<JobLoadBalancer> associations;
    private final Set<LoadBalancerTarget> targets;

    public InMemoryLoadBalancerStore() {
        associations = ConcurrentHashMap.newKeySet();
        targets = ConcurrentHashMap.newKeySet();
    }

    @Override
    public Observable<String> retrieveLoadBalancersForJob(String jobId) {
        return Observable.defer(() -> Observable.from(associations)
                .filter(jobLoadBalancer -> jobLoadBalancer.getJobId().equals(jobId))
                .map(JobLoadBalancer::getLoadBalancerId)
                .distinct());
    }

    @Override
    public Completable addLoadBalancer(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> associations.add(jobLoadBalancer));
    }

    @Override
    public Completable removeLoadBalancer(JobLoadBalancer jobLoadBalancer) {
        return Completable.fromAction(() -> associations.remove(jobLoadBalancer));
    }

    @Override
    public Observable<LoadBalancerTarget> retrieveTargets(JobLoadBalancer jobLoadBalancer) {
        return Observable.defer(() -> Observable.from(targets)
                .filter(target -> target.getJobLoadBalancer().equals(jobLoadBalancer))
        );
    }

    @Override
    public Completable updateTargets(Collection<LoadBalancerTarget> update) {
        if (CollectionsExt.isNullOrEmpty(update)) {
            return Completable.complete();
        }
        return Completable.fromAction(() -> targets.addAll(update));
    }

    @Override
    public Completable removeTargets(Collection<LoadBalancerTarget> remove) {
        if (CollectionsExt.isNullOrEmpty(remove)) {
            return Completable.complete();
        }
        return Completable.fromAction(() -> targets.removeAll(remove));
    }
}
