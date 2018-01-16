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

package io.netflix.titus.master.loadbalancer.endpoint.grpc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import io.netflix.titus.api.model.Pagination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.runtime.endpoint.common.grpc.CommonGrpcModelConverters.toGrpcPagination;

/**
 * Collection of functions to convert load balancer models from internal to gRPC formats.
 */
public final class GrpcModelConverters {
    private static Logger logger = LoggerFactory.getLogger(GrpcModelConverters.class);

    public static GetAllLoadBalancersResult toGetAllLoadBalancersResult(List<JobLoadBalancer> jobLoadBalancerList, Pagination runtimePagination) {
        GetAllLoadBalancersResult.Builder allLoadBalancersResult = GetAllLoadBalancersResult.newBuilder();
        allLoadBalancersResult.setPagination(toGrpcPagination(runtimePagination));

        // We expect the list to be in a sorted-by-jobId order and iterate as such
        GetJobLoadBalancersResult.Builder getJobLoadBalancersResultBuilder = GetJobLoadBalancersResult.newBuilder();
        Set<String> addedJobIds = new HashSet<>();
        for (JobLoadBalancer jobLoadBalancer : jobLoadBalancerList) {
            String jobId = jobLoadBalancer.getJobId();

            // Check if we're processing a new Job ID
            if (!addedJobIds.contains(jobId)) {
                // Add any previous JobID's result if it existed
                if (getJobLoadBalancersResultBuilder.getLoadBalancersBuilderList().size() > 0) {
                    allLoadBalancersResult.addJobLoadBalancers(getJobLoadBalancersResultBuilder.build());
                }
                getJobLoadBalancersResultBuilder = GetJobLoadBalancersResult.newBuilder()
                        .setJobId(jobId);
                addedJobIds.add(jobId);
            }
            getJobLoadBalancersResultBuilder.addLoadBalancers(LoadBalancerId.newBuilder().setId(jobLoadBalancer.getLoadBalancerId()).build());
        }

        if (getJobLoadBalancersResultBuilder.getLoadBalancersBuilderList().size() > 0) {
            allLoadBalancersResult.addJobLoadBalancers(getJobLoadBalancersResultBuilder.build());
        }
        return allLoadBalancersResult.build();
    }
}
