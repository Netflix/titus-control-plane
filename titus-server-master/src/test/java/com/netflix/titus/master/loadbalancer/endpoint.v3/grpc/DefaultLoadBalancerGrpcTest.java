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

package com.netflix.titus.master.loadbalancer.endpoint.v3.grpc;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.master.loadbalancer.service.LoadBalancerTests;
import com.netflix.titus.api.loadbalancer.service.LoadBalancerService;
import com.netflix.titus.master.loadbalancer.endpoint.grpc.DefaultLoadBalancerServiceGrpc;
import com.netflix.titus.master.loadbalancer.service.LoadBalancerTests;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.loadbalancer.service.LoadBalancerTests.getMockLoadBalancerService;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class DefaultLoadBalancerGrpcTest {
    private static Logger logger = LoggerFactory.getLogger(DefaultLoadBalancerGrpcTest.class);
    private DefaultLoadBalancerServiceGrpc serviceGrpc;

    @Before
    public void setUp() throws Exception {
        LoadBalancerService loadBalancerService = LoadBalancerTests.getMockLoadBalancerService();
        serviceGrpc = new DefaultLoadBalancerServiceGrpc(loadBalancerService);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testGetLoadBalancrs() throws Exception {
        String jobIdStr = "Titus-123";
        Set<LoadBalancerId> loadBalancerIds = LoadBalancerTests.getLoadBalancersForJob(jobIdStr, getJobLoadBalancers);
        assertThat(loadBalancerIds.size()).isEqualTo(0);
    }

    @Test
    public void testSetAndGetAndRmLoadBalancers() throws Exception {
        int numJobs = 5;
        int numLoadBalancers = 10;

        Map<String, Set<LoadBalancerId>> jobIdToLoadBalancersMap =
                LoadBalancerTests.putLoadBalancersPerJob(numJobs, numLoadBalancers, putLoadBalancerWithJobId);

        // For each job, query the load balancers and check that they match.
        jobIdToLoadBalancersMap.forEach((jobId, loadBalancerIdSet) -> {
            Set<LoadBalancerId> getIdSet = LoadBalancerTests.getLoadBalancersForJob(jobId, getJobLoadBalancers);
            logger.info("Checking that Job {} LB IDs {} match expected IDs {}", jobId, getIdSet, loadBalancerIdSet);
            assertThat(loadBalancerIdSet.equals(getIdSet)).isTrue();
        });

        // Remove the load balancers for each job
        jobIdToLoadBalancersMap.forEach((jobId, loadBalancerIdSet) -> {
            loadBalancerIdSet.forEach(loadBalancerId -> {
                logger.info("Removing load balancer {} from Job {}", loadBalancerId.getId(), jobId);
                LoadBalancerTests.removeLoadBalancerFromJob(jobId, loadBalancerId, removeLoadBalancers);
            });
        });

        // Check that there are no load balancers left
        jobIdToLoadBalancersMap.forEach((jobId, loadBalancerIdSet) -> {
            assertThat(LoadBalancerTests.getLoadBalancersForJob(jobId, getJobLoadBalancers).size()).isEqualTo(0);
            logger.info("Job {} has no more load balancers", jobId);
        });
    }

    private BiConsumer<AddLoadBalancerRequest, TestStreamObserver<Empty>> putLoadBalancerWithJobId = (request, addResponse) -> {
        serviceGrpc.addLoadBalancer(request, addResponse);
    };

    private BiConsumer<JobId, TestStreamObserver<GetJobLoadBalancersResult>> getJobLoadBalancers = (request, getResponse) -> {
        serviceGrpc.getJobLoadBalancers(request, getResponse);
    };

    private BiConsumer<RemoveLoadBalancerRequest, TestStreamObserver<Empty>> removeLoadBalancers = (request, removeResponse) -> {
        serviceGrpc.removeLoadBalancer(request, removeResponse);
    };
}
