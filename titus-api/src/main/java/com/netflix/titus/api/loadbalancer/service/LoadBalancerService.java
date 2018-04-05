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

package com.netflix.titus.api.loadbalancer.service;

import java.util.List;

import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.model.Page;
import com.netflix.titus.api.model.Pagination;
import com.netflix.titus.common.util.tuple.Pair;
import rx.Completable;
import rx.Observable;

public interface LoadBalancerService {
    /**
     * Returns all load balancers associated to a specific job.
     *
     * @param jobId
     * @return
     */
    Observable<String> getJobLoadBalancers(String jobId);

    /**
     * Blocking call that returns all job/load balancer associations.
     * As a blocking call, data must be served from cached/in-memory data and avoid doing external calls.
     *
     * @return
     */
    Pair<List<JobLoadBalancer>, Pagination> getAllLoadBalancers(Page page);

    /**
     * Adds a load balancer to an existing job.
     *
     * @param jobId
     * @param loadBalancerId
     * @return
     */
    Completable addLoadBalancer(String jobId, String loadBalancerId);

    /**
     * Removes a load balancer from an existing job.
     *
     * @param jobId
     * @param loadBalancerId
     * @return
     */
    Completable removeLoadBalancer(String jobId, String loadBalancerId);
}
