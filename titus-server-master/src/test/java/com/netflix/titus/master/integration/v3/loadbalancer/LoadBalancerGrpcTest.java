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

package com.netflix.titus.master.integration.v3.loadbalancer;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.loadbalancer.service.LoadBalancerTests;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.stack.EmbeddedTitusStack;
import com.netflix.titus.testkit.grpc.TestStreamObserver;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.loadbalancer.service.LoadBalancerTests.buildPageSupplier;
import static com.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters.basicMaster;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * These integration tests validate proper plumbing of gRPC client requests
 * through the Gateway and Master servers.
 */
@Category(IntegrationTest.class)
public class LoadBalancerGrpcTest extends BaseIntegrationTest {
    private final Logger logger = LoggerFactory.getLogger(LoadBalancerGrpcTest.class);
    private LoadBalancerServiceGrpc.LoadBalancerServiceStub client;

    public static final TitusStackResource titusStackResource = new TitusStackResource(EmbeddedTitusStack.aTitusStack()
            .withMaster(basicMaster(new SimulatedCloud()))
            .withDefaultGateway()
            .build());

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource);

    @Before
    public void setUp() throws Exception {
        client = titusStackResource.getOperations().getLoadBalancerGrpcClient();
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

    @Test
    public void testGetAllLoadBalancerPages() throws Exception {
        int numJobs = 75;
        int numLbs = 7;
        Map<String, Set<LoadBalancerId>> verificationMap = LoadBalancerTests.putLoadBalancersPerJob(numJobs, numLbs, putLoadBalancerWithJobId);

        int pageSize = 3;
        int currentPageNum = 0;
        GetAllLoadBalancersResult result;
        do {
            result = LoadBalancerTests.getAllLoadBalancers(buildPageSupplier(currentPageNum, pageSize), getAllLoadBalancers);

            result.getJobLoadBalancersList().forEach(
                    getJobLoadBalancersResult -> {
                        String jobId = getJobLoadBalancersResult.getJobId();
                        assertThat(verificationMap.containsKey(jobId)).isTrue();
                        getJobLoadBalancersResult.getLoadBalancersList().forEach(
                                loadBalancerId -> {
                                    // Mark the load balancer as checked
                                    assertThat(verificationMap.get(jobId).remove(loadBalancerId)).isTrue();
                                }
                        );
                    }
            );
            currentPageNum++;
        } while (result.getPagination().getHasMore());
        // Make sure that all of the data was checked
        verificationMap.forEach(
                (jobId, loadBalancerSet) -> {
                    assertThat(loadBalancerSet.isEmpty()).isTrue();
                }
        );
    }

    @Test
    public void testGetAllLoadBalancerPagesWithCursor() throws Exception {
        int numJobs = 75;
        int numLbs = 7;
        Map<String, Set<LoadBalancerId>> verificationMap = LoadBalancerTests.putLoadBalancersPerJob(numJobs, numLbs, putLoadBalancerWithJobId);

        int pageSize = 3;
        String cursor = "";
        GetAllLoadBalancersResult result;
        do {
            result = LoadBalancerTests.getAllLoadBalancers(buildPageSupplier(cursor, pageSize), getAllLoadBalancers);

            result.getJobLoadBalancersList().forEach(
                    getJobLoadBalancersResult -> {
                        String jobId = getJobLoadBalancersResult.getJobId();
                        assertThat(verificationMap.containsKey(jobId)).isTrue();
                        getJobLoadBalancersResult.getLoadBalancersList().forEach(
                                loadBalancerId -> {
                                    // Mark the load balancer as checked
                                    logger.info("checking lb {} exists for job {} - {}", loadBalancerId.getId(), jobId, verificationMap.get(jobId).contains(loadBalancerId));
                                    assertThat(verificationMap.get(jobId).remove(loadBalancerId)).isTrue();
                                }
                        );
                    }
            );
            cursor = result.getPagination().getCursor();
        } while (result.getPagination().getHasMore());
        // Make sure that all of the data was checked
        verificationMap.forEach(
                (jobId, loadBalancerSet) -> {
                    assertThat(loadBalancerSet.isEmpty()).isTrue();
                }
        );
    }


    private BiConsumer<AddLoadBalancerRequest, TestStreamObserver<Empty>> putLoadBalancerWithJobId = (request, addResponse) -> {
        client.addLoadBalancer(request, addResponse);
    };

    private BiConsumer<JobId, TestStreamObserver<GetJobLoadBalancersResult>> getJobLoadBalancers = (request, getResponse) -> {
        client.getJobLoadBalancers(request, getResponse);
    };

    private BiConsumer<GetAllLoadBalancersRequest, TestStreamObserver<GetAllLoadBalancersResult>> getAllLoadBalancers = (request, getResponse) -> {
        client.getAllLoadBalancers(request, getResponse);
    };

    private BiConsumer<RemoveLoadBalancerRequest, TestStreamObserver<Empty>> removeLoadBalancers = (request, removeResponse) -> {
        client.removeLoadBalancer(request, removeResponse);
    };
}
