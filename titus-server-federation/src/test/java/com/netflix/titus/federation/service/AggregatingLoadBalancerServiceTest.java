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
package com.netflix.titus.federation.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.Observable;
import rx.observers.AssertableSubscriber;

import static com.netflix.titus.testkit.junit.spring.SpringMockMvcUtil.JUNIT_REST_CALL_METADATA;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class AggregatingLoadBalancerServiceTest {
    private static final String JOB_1 = "job1";
    private static final String JOB_2 = "job2";
    private static final String LB_1 = "loadBalancer1";
    private static final String LB_2 = "loadBalancer2";
    private static final String LB_3 = "loadBalancer3";

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();

    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();

    AggregatingLoadbalancerService service;

    @Before
    public void setup() {
        CellConnector connector = mock(CellConnector.class);
        Map<Cell, ManagedChannel> cellMap = new HashMap<>();
        cellMap.put(new Cell("one", "1"), cellOne.getChannel());
        cellMap.put(new Cell("two", "2"), cellTwo.getChannel());
        when(connector.getChannels()).thenReturn(cellMap);
        when(connector.getChannelForCell(any())).then(invocation ->
                Optional.ofNullable(cellMap.get(invocation.getArgument(0)))
        );

        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(1000L);
        final AnonymousCallMetadataResolver anonymousCallMetadataResolver = new AnonymousCallMetadataResolver();
        final AggregatingCellClient aggregatingCellClient = new AggregatingCellClient(connector);

        service = new AggregatingLoadbalancerService(connector, anonymousCallMetadataResolver, grpcConfiguration, aggregatingCellClient,
                new AggregatingJobManagementServiceHelper(aggregatingCellClient, grpcConfiguration));
    }

    @Test
    public void getLoadBalancersForJob() {
        JobLoadBalancer jobLoadBalancer1 = new JobLoadBalancer(JOB_1, LB_1);
        JobLoadBalancer jobLoadBalancer2 = new JobLoadBalancer(JOB_2, LB_2);
        final CellWithLoadBalancers cellWithLoadBalancersOne = new CellWithLoadBalancers(singletonList(jobLoadBalancer1));
        final CellWithLoadBalancers cellWithLoadBalancersTwo = new CellWithLoadBalancers(singletonList(jobLoadBalancer2));
        cellOne.getServiceRegistry().addService(cellWithLoadBalancersOne);
        cellTwo.getServiceRegistry().addService(cellWithLoadBalancersTwo);

        final AssertableSubscriber<GetJobLoadBalancersResult> resultSubscriber = service.getLoadBalancers(JobId.newBuilder().setId(JOB_2).build(), JUNIT_REST_CALL_METADATA).test();
        resultSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors();
        final List<GetJobLoadBalancersResult> onNextEvents = resultSubscriber.getOnNextEvents();
        assertThat(onNextEvents.size()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getJobId()).isEqualTo(JOB_2);
        assertThat(onNextEvents.get(0).getLoadBalancersCount()).isEqualTo(1);
        assertThat(onNextEvents.get(0).getLoadBalancers(0).getId()).isEqualTo(LB_2);
    }

    @Test
    public void addLoadBalancer() {
        JobLoadBalancer jobLoadBalancer1 = new JobLoadBalancer(JOB_1, LB_1);
        JobLoadBalancer jobLoadBalancer2 = new JobLoadBalancer(JOB_2, LB_2);
        final CellWithLoadBalancers cellWithLoadBalancersOne = new CellWithLoadBalancers(singletonList(jobLoadBalancer1));
        final CellWithLoadBalancers cellWithLoadBalancersTwo = new CellWithLoadBalancers(new ArrayList<>(singletonList(jobLoadBalancer2)));

        final CellWithJobIds cellWithJobIdsOne = new CellWithJobIds(singletonList(JOB_1));
        final CellWithJobIds cellWithJobIdsTwo = new CellWithJobIds(singletonList(JOB_2));

        cellOne.getServiceRegistry().addService(cellWithLoadBalancersOne);
        cellOne.getServiceRegistry().addService(cellWithJobIdsOne);
        cellTwo.getServiceRegistry().addService(cellWithLoadBalancersTwo);
        cellTwo.getServiceRegistry().addService(cellWithJobIdsTwo);

        final AssertableSubscriber<Void> resultSubscriber = service.addLoadBalancer(
                AddLoadBalancerRequest.newBuilder().setJobId(JOB_2).setLoadBalancerId(LoadBalancerId.newBuilder().setId(LB_3).build()).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors();
        resultSubscriber.assertNoValues();
        resultSubscriber.assertCompleted();

        final AssertableSubscriber<GetJobLoadBalancersResult> jobResults = service.getLoadBalancers(
                JobId.newBuilder().setId(JOB_2).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        jobResults.assertNoErrors();
        final List<GetJobLoadBalancersResult> onNextEvents = jobResults.getOnNextEvents();
        assertThat(onNextEvents.size()).isEqualTo(1);
        final List<LoadBalancerId> loadBalancersList = onNextEvents.get(0).getLoadBalancersList();
        assertThat(loadBalancersList.size()).isEqualTo(2);

        final List<String> resultLoadBalancerIds = loadBalancersList.stream().map(LoadBalancerId::getId).collect(Collectors.toList());
        assertThat(resultLoadBalancerIds).contains(LB_2, LB_3);
    }

    @Test
    public void removeLoadBalancer() {
        JobLoadBalancer jobLoadBalancer1 = new JobLoadBalancer(JOB_1, LB_1);
        JobLoadBalancer jobLoadBalancer2 = new JobLoadBalancer(JOB_2, LB_2);
        JobLoadBalancer jobLoadBalancer3 = new JobLoadBalancer(JOB_2, LB_3);
        final CellWithLoadBalancers cellWithLoadBalancersOne = new CellWithLoadBalancers(singletonList(jobLoadBalancer1));
        final CellWithLoadBalancers cellWithLoadBalancersTwo = new CellWithLoadBalancers(new ArrayList<>(Arrays.asList(jobLoadBalancer2, jobLoadBalancer3)));

        final CellWithJobIds cellWithJobIdsOne = new CellWithJobIds(singletonList(JOB_1));
        final CellWithJobIds cellWithJobIdsTwo = new CellWithJobIds(singletonList(JOB_2));

        cellOne.getServiceRegistry().addService(cellWithLoadBalancersOne);
        cellOne.getServiceRegistry().addService(cellWithJobIdsOne);
        cellTwo.getServiceRegistry().addService(cellWithLoadBalancersTwo);
        cellTwo.getServiceRegistry().addService(cellWithJobIdsTwo);

        final AssertableSubscriber<Void> resultSubscriber = service.removeLoadBalancer(
                RemoveLoadBalancerRequest.newBuilder().setJobId(JOB_2).setLoadBalancerId(LoadBalancerId.newBuilder().setId(LB_2).build()).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors();
        resultSubscriber.assertNoValues();
        resultSubscriber.assertCompleted();

        final AssertableSubscriber<GetJobLoadBalancersResult> jobResults = service.getLoadBalancers(
                JobId.newBuilder().setId(JOB_2).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        jobResults.awaitValueCount(1, 1, TimeUnit.SECONDS);
        jobResults.assertNoErrors();
        final List<GetJobLoadBalancersResult> onNextEvents = jobResults.getOnNextEvents();
        assertThat(onNextEvents.size()).isEqualTo(1);
        final List<LoadBalancerId> loadBalancersList = onNextEvents.get(0).getLoadBalancersList();
        assertThat(loadBalancersList.size()).isEqualTo(1);
        assertThat(loadBalancersList.get(0).getId()).isEqualTo(LB_3);
    }

    @Test
    public void getAllLoadBalancersNoPagination() {
        JobLoadBalancer jobLoadBalancer1 = new JobLoadBalancer(JOB_1, LB_1);
        JobLoadBalancer jobLoadBalancer2 = new JobLoadBalancer(JOB_2, LB_2);
        final CellWithLoadBalancers cellWithLoadBalancersOne = new CellWithLoadBalancers(singletonList(jobLoadBalancer1));
        final CellWithLoadBalancers cellWithLoadBalancersTwo = new CellWithLoadBalancers(singletonList(jobLoadBalancer2));
        cellOne.getServiceRegistry().addService(cellWithLoadBalancersOne);
        cellTwo.getServiceRegistry().addService(cellWithLoadBalancersTwo);

        final AssertableSubscriber<GetAllLoadBalancersResult> resultSubscriber = service.getAllLoadBalancers(
                GetAllLoadBalancersRequest.newBuilder().setPage(Page.newBuilder().setPageNumber(0).setPageSize(10)).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors();
        final List<GetAllLoadBalancersResult> onNextEvents = resultSubscriber.getOnNextEvents();
        assertThat(onNextEvents).hasSize(1);
        assertThat(onNextEvents.get(0).getJobLoadBalancersCount()).isEqualTo(2);
    }


    @Test
    public void getAllLoadBalancersWithPagination() {
        final List<JobLoadBalancer> jobLoadBalancersOne = Observable.range(1, 5)
                .map(i -> new JobLoadBalancer(JOB_1, String.format("LB_1_%d", i.intValue())))
                .toList().toBlocking().first();

        final List<JobLoadBalancer> jobLoadBalancersTwo = Observable.range(1, 5)
                .map(i -> new JobLoadBalancer(JOB_2, String.format("LB_2_%d", i.intValue())))
                .toList().toBlocking().first();

        final CellWithLoadBalancers cellWithLoadBalancersOne = new CellWithLoadBalancers(jobLoadBalancersOne);
        final CellWithLoadBalancers cellWithLoadBalancersTwo = new CellWithLoadBalancers(jobLoadBalancersTwo);
        cellOne.getServiceRegistry().addService(cellWithLoadBalancersOne);
        cellTwo.getServiceRegistry().addService(cellWithLoadBalancersTwo);

        final List<JobLoadBalancer> results = ServiceTests.walkAllPages(
                2,
                request -> service.getAllLoadBalancers(request, JUNIT_REST_CALL_METADATA),
                page -> GetAllLoadBalancersRequest.newBuilder().setPage(page).build(),
                GetAllLoadBalancersResult::getPagination,
                this::buildJobLoadBalancerList
        );

        assertThat(results).hasSize(10);
    }

    @Test
    public void getLoadBalancersWithOneFailingCell() {
        JobLoadBalancer jobLoadBalancer1 = new JobLoadBalancer(JOB_1, LB_1);
        JobLoadBalancer jobLoadBalancer2 = new JobLoadBalancer(JOB_1, LB_2);
        final CellWithLoadBalancers cellWithLoadBalancersOne = new CellWithLoadBalancers(
                Arrays.asList(jobLoadBalancer1, jobLoadBalancer2));

        cellOne.getServiceRegistry().addService(cellWithLoadBalancersOne);
        cellTwo.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.INTERNAL));

        final AssertableSubscriber<GetAllLoadBalancersResult> resultSubscriber = service.getAllLoadBalancers(
                GetAllLoadBalancersRequest.newBuilder().setPage(Page.newBuilder().setPageSize(10)).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        resultSubscriber.assertNoValues();
        final List<Throwable> onErrorEvents = resultSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents).hasSize(1);
        assertThat(Status.fromThrowable(onErrorEvents.get(0))).isEqualTo(Status.INTERNAL);
    }

    @Test
    public void getLoadBalancersWithTwoFailingCell() {
        cellOne.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.UNAVAILABLE));
        cellTwo.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.INTERNAL));

        final AssertableSubscriber<GetAllLoadBalancersResult> resultSubscriber = service.getAllLoadBalancers(
                GetAllLoadBalancersRequest.newBuilder().setPage(Page.newBuilder().setPageSize(10)).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        resultSubscriber.assertNoValues();
        final List<Throwable> onErrorEvents = resultSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents).hasSize(1);
        assertThat(Status.fromThrowable(onErrorEvents.get(0))).isEqualTo(Status.INTERNAL);
    }

    @Test
    public void getJobLoadBalancersWithOneFailingCell() {
        JobLoadBalancer jobLoadBalancer1 = new JobLoadBalancer(JOB_1, LB_1);
        JobLoadBalancer jobLoadBalancer2 = new JobLoadBalancer(JOB_1, LB_2);
        final CellWithLoadBalancers cellWithLoadBalancersOne = new CellWithLoadBalancers(
                Arrays.asList(jobLoadBalancer1, jobLoadBalancer2));

        cellOne.getServiceRegistry().addService(cellWithLoadBalancersOne);
        cellTwo.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.INTERNAL));

        final AssertableSubscriber<GetJobLoadBalancersResult> resultSubscriber = service.getLoadBalancers(
                JobId.newBuilder().setId(JOB_1).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitValueCount(1, 1, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors();
        final List<GetJobLoadBalancersResult> onNextEvents = resultSubscriber.getOnNextEvents();
        assertThat(onNextEvents).hasSize(1);
        final List<LoadBalancerId> loadBalancersList = onNextEvents.get(0).getLoadBalancersList();
        final List<String> resultLoadBalancers = loadBalancersList.stream().map(loadBalancerId -> loadBalancerId.getId()).collect(Collectors.toList());
        assertThat(resultLoadBalancers).contains(LB_1, LB_2);
    }

    @Test
    public void getJobLoadBalancersWithTwoFailingCell() {
        cellOne.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.NOT_FOUND));
        cellTwo.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.INTERNAL));

        final AssertableSubscriber<GetJobLoadBalancersResult> resultSubscriber = service.getLoadBalancers(
                JobId.newBuilder().setId(JOB_1).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        resultSubscriber.assertNoValues();
        final List<Throwable> onErrorEvents = resultSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents).hasSize(1);
        assertThat(Status.fromThrowable(onErrorEvents.get(0))).isEqualTo(Status.INTERNAL);
    }

    @Test
    public void getJobLoadBalancersInvalidJobId() {
        cellOne.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.NOT_FOUND));
        cellTwo.getServiceRegistry().addService(new CellWithFailingLoadBalancers(Status.NOT_FOUND));

        final AssertableSubscriber<GetJobLoadBalancersResult> resultSubscriber = service.getLoadBalancers(
                JobId.newBuilder().setId(JOB_1).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        resultSubscriber.assertNoValues();
        final List<Throwable> onErrorEvents = resultSubscriber.getOnErrorEvents();
        assertThat(onErrorEvents).hasSize(1);
        assertThat(Status.fromThrowable(onErrorEvents.get(0))).isEqualTo(Status.NOT_FOUND);
    }

    @Test
    public void getJobLoadBalancersNoAssociations() {
        cellOne.getServiceRegistry().addService(new CellWithLoadBalancers(Collections.emptyList()));
        cellTwo.getServiceRegistry().addService(new CellWithLoadBalancers(Collections.emptyList()));

        final AssertableSubscriber<GetJobLoadBalancersResult> resultSubscriber = service.getLoadBalancers(
                JobId.newBuilder().setId(JOB_1).build(),
                JUNIT_REST_CALL_METADATA
        ).test();
        resultSubscriber.awaitTerminalEvent(1, TimeUnit.SECONDS);
        resultSubscriber.assertNoErrors().assertCompleted();
        resultSubscriber.assertValueCount(1);
        assertThat(resultSubscriber.getOnNextEvents().get(0).getLoadBalancersCount()).isZero();
    }

    private List<JobLoadBalancer> buildJobLoadBalancerList(GetAllLoadBalancersResult getAllLoadBalancersResult) {
        return getAllLoadBalancersResult.getJobLoadBalancersList().stream()
                .flatMap(jobLoadBalancersResult -> {
                    final String jobId = jobLoadBalancersResult.getJobId();
                    return jobLoadBalancersResult.getLoadBalancersList().stream()
                            .map(loadBalancerId -> new JobLoadBalancer(jobId, loadBalancerId.getId()));
                }).collect(Collectors.toList());
    }
}
