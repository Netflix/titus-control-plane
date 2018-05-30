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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.protobuf.util.Durations;
import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import com.netflix.titus.federation.startup.GrpcConfiguration;
import com.netflix.titus.grpc.protogen.HealthCheckRequest;
import com.netflix.titus.grpc.protogen.HealthCheckResponse;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.Details;
import com.netflix.titus.grpc.protogen.HealthCheckResponse.ServerStatus;
import com.netflix.titus.grpc.protogen.HealthGrpc;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import rx.observers.AssertableSubscriber;

import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.NOT_SERVING;
import static com.netflix.titus.grpc.protogen.HealthCheckResponse.ServingStatus.SERVING;
import static io.grpc.Status.Code.DEADLINE_EXCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class AggregatingHealthServiceTest {

    private final TestClock clock = Clocks.test();

    @Rule
    public final GrpcServerRule cellOne = new GrpcServerRule().directExecutor();

    @Rule
    public final GrpcServerRule cellTwo = new GrpcServerRule().directExecutor();

    AggregatingHealthService service;
    private final CellConnector connector = mock(CellConnector.class);

    @Before
    public void setup() {
        Map<Cell, ManagedChannel> cellMap = new HashMap<>();
        cellMap.put(new Cell("one", "1"), cellOne.getChannel());
        cellMap.put(new Cell("two", "2"), cellTwo.getChannel());
        when(connector.getChannels()).thenReturn(cellMap);
        when(connector.getChannelForCell(any())).then(invocation ->
                Optional.ofNullable(cellMap.get(invocation.getArgument(0)))
        );

        GrpcConfiguration grpcConfiguration = mock(GrpcConfiguration.class);
        when(grpcConfiguration.getRequestTimeoutMs()).thenReturn(1000L);
        AnonymousCallMetadataResolver anonymousCallMetadataResolver = new AnonymousCallMetadataResolver();
        AggregatingCellClient aggregatingCellClient = new AggregatingCellClient(connector);
        service = new AggregatingHealthService(aggregatingCellClient, anonymousCallMetadataResolver, grpcConfiguration);
    }

    @Test
    public void allCellsOK() {
        cellOne.getServiceRegistry().addService(new CellWithHealthStatus(ok("one")));
        cellTwo.getServiceRegistry().addService(new CellWithHealthStatus(ok("two")));

        AssertableSubscriber<HealthCheckResponse> subscriber = service.check(HealthCheckRequest.newBuilder().build()).test();
        subscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);
        HealthCheckResponse response = subscriber.getOnNextEvents().get(0);
        assertThat(response.getStatus()).isEqualTo(SERVING);
        assertThat(response.getDetailsCount()).isEqualTo(2);
        assertThat(response.getDetails(0).hasDetails()).isTrue();
        assertThat(response.getDetails(1).hasDetails()).isTrue();
        Set<String> cellsSeen = response.getDetailsList().stream()
                .map(s -> s.getDetails().getCell())
                .collect(Collectors.toSet());
        assertThat(cellsSeen).contains("one", "two");
    }

    @Test
    public void singleCell() {
        reset(connector);
        when(connector.getChannels()).thenReturn(Collections.singletonMap(
                new Cell("one", "1"), cellOne.getChannel()
        ));
        when(connector.getChannelForCell(any())).thenReturn(Optional.of(cellOne.getChannel()));

        HealthCheckResponse one = HealthCheckResponse.newBuilder()
                .setStatus(NOT_SERVING)
                .addDetails(ServerStatus.newBuilder()
                        .setDetails(Details.newBuilder()
                                .setCell("one")
                                .setLeader(false)
                                .setActive(true)
                                .setUptime(Durations.fromMillis(clock.wallTime()))
                                .build())
                        .build())
                .build();
        cellOne.getServiceRegistry().addService(new CellWithHealthStatus(one));

        AssertableSubscriber<HealthCheckResponse> subscriber = service.check(HealthCheckRequest.newBuilder().build()).test();
        subscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);
        HealthCheckResponse response = subscriber.getOnNextEvents().get(0);
        assertThat(response.getStatus()).isEqualTo(NOT_SERVING);
        assertThat(response.getDetailsCount()).isEqualTo(1);
        assertThat(response.getDetails(0).hasDetails()).isTrue();
        assertThat(response.getDetails(0).getDetails().getCell()).isEqualTo("one");
        assertThat(response.getDetails(0).getDetails().getLeader()).isFalse();
    }

    @Test
    public void oneCellFailing() {
        cellOne.getServiceRegistry().addService(new CellWithHealthStatus(ok("one")));
        cellTwo.getServiceRegistry().addService(new CellWithHealthStatus(failing("two")));

        AssertableSubscriber<HealthCheckResponse> subscriber = service.check(HealthCheckRequest.newBuilder().build()).test();
        subscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);
        HealthCheckResponse response = subscriber.getOnNextEvents().get(0);
        assertThat(response.getStatus()).isEqualTo(NOT_SERVING);
        assertThat(response.getDetailsCount()).isEqualTo(2);
        assertThat(response.getDetails(0).hasDetails()).isTrue();
        assertThat(response.getDetails(1).hasDetails()).isTrue();
        Set<String> cellsSeen = response.getDetailsList().stream()
                .map(s -> s.getDetails().getCell())
                .collect(Collectors.toSet());
        assertThat(cellsSeen).contains("one", "two");
    }

    @Test
    public void grpcErrors() {
        cellOne.getServiceRegistry().addService(new CellWithHealthStatus(ok("one")));
        cellTwo.getServiceRegistry().addService(new HealthGrpc.HealthImplBase() {
            @Override
            public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
                responseObserver.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
            }
        });

        AssertableSubscriber<HealthCheckResponse> subscriber = service.check(HealthCheckRequest.newBuilder().build()).test();
        subscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);
        HealthCheckResponse response = subscriber.getOnNextEvents().get(0);
        assertThat(response.getStatus()).isEqualTo(NOT_SERVING);
        assertThat(response.getDetailsCount()).isEqualTo(2);
        List<ServerStatus> errors = response.getDetailsList().stream()
                .filter(ServerStatus::hasError)
                .collect(Collectors.toList());
        assertThat(errors).hasSize(1);
        assertThat(errors.get(0).getError().getCell()).isEqualTo("two");
        assertThat(errors.get(0).getError().getErrorCode()).isEqualTo(DEADLINE_EXCEEDED.toString());
    }

    @Test
    public void allCellsFailing() {
        cellOne.getServiceRegistry().addService(new CellWithHealthStatus(failing("one")));
        cellTwo.getServiceRegistry().addService(new CellWithHealthStatus(failing("two")));

        AssertableSubscriber<HealthCheckResponse> subscriber = service.check(HealthCheckRequest.newBuilder().build()).test();
        subscriber.awaitTerminalEvent(10, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        subscriber.assertValueCount(1);
        HealthCheckResponse response = subscriber.getOnNextEvents().get(0);
        assertThat(response.getStatus()).isEqualTo(NOT_SERVING);
        assertThat(response.getDetailsCount()).isEqualTo(2);
        assertThat(response.getDetails(0).hasDetails()).isTrue();
        assertThat(response.getDetails(1).hasDetails()).isTrue();
        Set<String> cellsSeen = response.getDetailsList().stream()
                .map(s -> s.getDetails().getCell())
                .collect(Collectors.toSet());
        assertThat(cellsSeen).contains("one", "two");
    }

    private HealthCheckResponse ok(String cellName) {
        return HealthCheckResponse.newBuilder()
                .setStatus(SERVING)
                .addDetails(ServerStatus.newBuilder()
                        .setDetails(Details.newBuilder()
                                .setCell(cellName)
                                .setLeader(true)
                                .setActive(true)
                                .setUptime(Durations.fromMillis(clock.wallTime()))
                                .build())
                        .build())
                .build();
    }

    private HealthCheckResponse failing(String cellName) {
        return HealthCheckResponse.newBuilder()
                .setStatus(NOT_SERVING)
                .addDetails(ServerStatus.newBuilder()
                        .setDetails(Details.newBuilder()
                                .setCell(cellName)
                                .setLeader(true)
                                .setActive(false)
                                .setUptime(Durations.fromMillis(clock.wallTime()))
                                .build())
                        .build())
                .build();
    }
}