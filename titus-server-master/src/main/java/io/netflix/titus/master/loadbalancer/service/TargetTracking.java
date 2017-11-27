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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.model.TargetState;
import io.netflix.titus.common.util.CollectionsExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

/**
 * In-memory state of load balancer targets currently being tracked
 */
class TargetTracking {
    private static Logger logger = LoggerFactory.getLogger(TargetTracking.class);

    private final ConcurrentMap<LoadBalancerTarget, LoadBalancerTarget.State> targets = new ConcurrentHashMap<>();

    Observable<TargetState> retrieveTargets(JobLoadBalancer jobLoadBalancer) {
        // TODO: index by jobLoadBalancer
        return Observable.defer(() -> Observable.from(targets.entrySet())
                .filter(entry -> entry.getKey().getJobLoadBalancer().equals(jobLoadBalancer))
                .map(entry -> new TargetState(entry.getKey(), entry.getValue()))
        );
    }

    Completable updateTargets(Map<LoadBalancerTarget, LoadBalancerTarget.State> update) {
        if (CollectionsExt.isNullOrEmpty(update)) {
            return Completable.complete();
        }
        return Completable.fromAction(() -> update.forEach(targets::put));
    }

    Completable removeTargets(Collection<LoadBalancerTarget> remove) {
        if (CollectionsExt.isNullOrEmpty(remove)) {
            return Completable.complete();
        }
        return Completable.fromAction(() -> targets.keySet().removeAll(remove));
    }
}
