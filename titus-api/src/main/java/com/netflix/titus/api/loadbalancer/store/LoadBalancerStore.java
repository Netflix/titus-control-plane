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

package com.netflix.titus.api.loadbalancer.store;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTarget;
import com.netflix.titus.api.loadbalancer.model.LoadBalancerTargetState;
import reactor.core.publisher.Mono;
import rx.Completable;
import rx.Observable;

public interface LoadBalancerStore {

    /**
     * Returns an observable stream of load balancers in associated state for a Job.
     */
    Observable<JobLoadBalancer> getAssociatedLoadBalancersForJob(String jobId);

    /**
     * Adds a new or updates an existing load balancer with the provided state.
     */
    Completable addOrUpdateLoadBalancer(JobLoadBalancer jobLoadBalancer, JobLoadBalancer.State state);

    /**
     * Removes a load balancer associated with a job.
     */
    Completable removeLoadBalancer(JobLoadBalancer jobLoadBalancer);

    /**
     * Blocking call that returns the current snapshot set of load balancers associated with for a Job.
     * As a blocking call, data must be served from cached/in-memory data and avoid doing external calls.
     */
    Set<JobLoadBalancer> getAssociatedLoadBalancersSetForJob(String jobId);

    /**
     * Blocking call that returns the number of load balancers associated with a job.
     * As a blocking call, data must be served from cached/in-memory data and avoid doing external calls.
     *
     * @return Returns 0 even if jobId does not exist.
     */
    int getNumLoadBalancersForJob(String jobId);

    /**
     * Blocking call that returns a (snapshot) view of the existing job/loadBalancer associations. It must work out of
     * cached data in-memory only, and avoid doing external calls.
     */
    List<JobLoadBalancerState> getAssociations();

    /**
     * Blocking call the returns the current snapshot page of the given offset/size of * all load balancers. As a
     * blocking call, data must be served from cached/in-memory data and avoid doing external calls.
     */
    List<JobLoadBalancer> getAssociationsPage(int offset, int limit);

    /**
     * Adds a or updates target with the provided state.
     */
    Mono<Void> addOrUpdateTarget(LoadBalancerTarget target, LoadBalancerTarget.State state);

    /**
     * Removes one or more targets associated with a load balancer
     */
    Mono<Void> removeTargets(Collection<LoadBalancerTarget> targets);

    /**
     * Blocking call that returns a (snapshot) view of the existing load balancer targets, indexed by load balancer id.
     * It must work out of cached data in-memory only, and avoid doing external calls.
     */
    Collection<LoadBalancerTargetState> getTargets();
}
