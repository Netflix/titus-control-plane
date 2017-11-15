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

package io.netflix.titus.api.loadbalancer.store;

import java.util.Collection;
import java.util.Map;

import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import io.netflix.titus.api.loadbalancer.model.TargetState;
import rx.Completable;
import rx.Observable;

/**
 * Interface for storing load balancer target state information.
 */
public interface TargetStore {
    /**
     * Returns the current state of the targets for a Job Load Balancer.
     * @param jobLoadBalancer
     * @return
     */
    Observable<TargetState> retrieveTargets(JobLoadBalancer jobLoadBalancer);

    /**
     * Updates the states of a collection of load balancer targets. These collection may include
     * new targets and/or update existing targets.
     * @param targets may be null or empty, in which case this is a noop
     */
    Completable updateTargets(Map<LoadBalancerTarget, LoadBalancerTarget.State> targets);

    /**
     * Removes specific targets from their associated load balancer.
     * @param targets
     * @return
     */
    Completable removeTargets(Collection<LoadBalancerTarget> targets);
}
