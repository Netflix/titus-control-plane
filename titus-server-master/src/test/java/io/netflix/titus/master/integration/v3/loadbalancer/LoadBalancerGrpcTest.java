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

package io.netflix.titus.master.integration.v3.loadbalancer;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetLoadBalancerResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.netflix.titus.master.loadbalancer.service.LoadBalancerTests;
import io.netflix.titus.testkit.grpc.TestStreamObserver;
import io.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * These integration tests validate proper plumbing of gRPC client requests
 * through the Gateway and Master servers.
 */
public class LoadBalancerGrpcTest {
    private final Logger log = LoggerFactory.getLogger(LoadBalancerGrpcTest.class);
    private LoadBalancerServiceGrpc.LoadBalancerServiceStub client;

    @Rule
    public static final TitusStackResource titusStackResource = TitusStackResource.aDefaultStack();

    @Before
    public void setUp() throws Exception {
        client = titusStackResource.getGateway().getLoadBalancerGrpcClient();
        // client = titusStackResource.getMaster().getLoadBalancerGrpcClient();
    }

    @Test
    public void testPutLoadBalancer() throws Exception {
        LoadBalancerTests.putLoadBalancersPerJob(1, 1, putLoadBalancerWithJobId);
    }

    @Test
    public void testGetLoadBalancer() throws Exception {
        String jobId = "Titus-123";
        Set<LoadBalancerId> loadBalancerIds = LoadBalancerTests.getLoadBalancersForJob(jobId, getJobLoadBalancers);
        assertThat(loadBalancerIds.size()).isEqualTo(0);
    }

    @Test
    public void testRmLoadBalancer() throws Exception {
        Map<String, Set<LoadBalancerId>> jobIdToLoadBalancersMap = LoadBalancerTests.putLoadBalancersPerJob(10, 50, putLoadBalancerWithJobId);

        // Remove the load balancers for each job
        jobIdToLoadBalancersMap.forEach((jobId, loadBalancerIdSet) -> {
            loadBalancerIdSet.forEach(loadBalancerId -> {
                LoadBalancerTests.removeLoadBalancerFromJob(jobId, loadBalancerId, removeLoadBalancers);
            });
        });

        // Check that there are no load balancers left
        jobIdToLoadBalancersMap.forEach((jobId, loadBalancerIdSet) -> {
            assertThat(LoadBalancerTests.getLoadBalancersForJob(jobId, getJobLoadBalancers).size()).isEqualTo(0);
        });
    }

    private BiConsumer<AddLoadBalancerRequest, TestStreamObserver<Empty>> putLoadBalancerWithJobId = (request, addResponse) -> {
        client.addLoadBalancer(request, addResponse);
    };

    private BiConsumer<JobId, TestStreamObserver<GetLoadBalancerResult>> getJobLoadBalancers = (request, getResponse) -> {
        client.getJobLoadBalancers(request, getResponse);
    };

    private BiConsumer<RemoveLoadBalancerRequest, TestStreamObserver<Empty>> removeLoadBalancers = (request, removeResponse) -> {
        client.removeLoadBalancer(request, removeResponse);
    };
}
