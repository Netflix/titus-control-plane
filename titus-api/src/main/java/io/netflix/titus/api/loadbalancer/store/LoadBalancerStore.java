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

import java.util.List;

import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancerState;
import rx.Completable;
import rx.Observable;

public interface LoadBalancerStore {
    String LOAD_BALANCER_SANITIZER = "loadbalancer";

    /**
     * Returns all load balancers associated with a job.
     *
     * @param jobId
     * @return
     */
    Observable<JobLoadBalancerState> retrieveLoadBalancersForJob(String jobId);

    /**
     * Adds a new or updates an existing load balancer with the provided state.
     *
     * @param jobLoadBalancer
     * @param state
     * @return
     */
    Completable addOrUpdateLoadBalancer(JobLoadBalancer jobLoadBalancer, JobLoadBalancer.State state);

    /**
     * Removes a load balancer associated with a job.
     *
     * @param jobLoadBalancer
     * @return
     */
    Completable removeLoadBalancer(JobLoadBalancer jobLoadBalancer);

    /**
     * Blocking call that returns the number of load balancers associated with a job.
     *
     * @param jobId
     * @return Returns 0 even if jobId does not exist.
     */
    int getNumLoadBalancersForJob(String jobId);

    /**
     * Blocking call that returns a (snapshot) view of the existing job/loadBalancer associations. It must work out of
     * cached data in-memory only, and avoid doing external calls.
     */
    List<JobLoadBalancerState> getAssociations();
}
