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

package com.netflix.titus.runtime.connector.common.reactor;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.Empty;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.connector.ChannelTunablesConfiguration;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleGrpcCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import com.netflix.titus.testing.SampleGrpcService.SampleContainer;
import com.netflix.titus.testing.SampleServiceGrpc;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.api.jobmanager.service.JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

public class ReactorToGrpcClientBuilderTest {

    private static final Duration TIMEOUT_DURATION = Duration.ofSeconds(30);

    private static final SampleContainer HELLO = SampleContainer.newBuilder().setStringValue("Hello").build();

    private static final long TIMEOUT_MS = 30_000;

    private ManagedChannel channel;
    private Server server;
    private SampleServiceImpl sampleService;
    private SampleServiceReactApi client;

    @Before
    public void setUp() throws Exception {
        this.sampleService = new SampleServiceImpl();
        this.server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(sampleService, new V3HeaderInterceptor()))
                .build()
                .start();
        this.channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();

        this.client = ReactorToGrpcClientBuilder.newBuilder(SampleServiceReactApi.class, SampleServiceGrpc.newStub(channel), SampleServiceGrpc.getServiceDescriptor())
                .withTimeout(TIMEOUT_DURATION)
                .withStreamingTimeout(Duration.ofMillis(ChannelTunablesConfiguration.DEFAULT_STREAMING_TIMEOUT_MS))
                .withCallMetadataResolver(new AnonymousCallMetadataResolver())
                .build();
    }

    @After
    public void tearDown() {
        ExceptionExt.silent(channel, ManagedChannel::shutdownNow);
        ExceptionExt.silent(server, Server::shutdownNow);
    }

    @Test(timeout = 30_000)
    public void testMonoGet() {
        assertThat(client.getOneValue().block().getStringValue()).isEqualTo("Hello");
    }

    @Test(timeout = 30_000)
    public void testMonoGetWithCallMetadata() {
        sampleService.expectedCallerId = GRPC_REPLICATOR_CALL_METADATA.getCallerId();
        assertThat(client.getOneValue(GRPC_REPLICATOR_CALL_METADATA).block().getStringValue()).isEqualTo("Hello");
    }

    @Test(timeout = 30_000)
    public void testMonoSet() {
        assertThat(client.setOneValue(HELLO).block()).isNull();
        assertThat(sampleService.lastSet).isEqualTo(HELLO);
    }

    @Test(timeout = 30_000)
    public void testMonoSetWithCallMetadata() {
        sampleService.expectedCallerId = GRPC_REPLICATOR_CALL_METADATA.getCallerId();
        assertThat(client.setOneValue(HELLO, GRPC_REPLICATOR_CALL_METADATA).block()).isNull();
        assertThat(sampleService.lastSet).isEqualTo(HELLO);
    }

    @Test(timeout = 30_000)
    public void testFlux() throws InterruptedException {
        TitusRxSubscriber<SampleContainer> subscriber = new TitusRxSubscriber<>();
        client.stream().subscribe(subscriber);
        checkTestFlux(subscriber);
    }

    @Test(timeout = 30_000)
    public void testFluxWithCallMetadata() throws InterruptedException {
        sampleService.expectedCallerId = GRPC_REPLICATOR_CALL_METADATA.getCallerId();
        TitusRxSubscriber<SampleContainer> subscriber = new TitusRxSubscriber<>();

        client.stream(GRPC_REPLICATOR_CALL_METADATA).subscribe(subscriber);
        checkTestFlux(subscriber);
    }

    private void checkTestFlux(TitusRxSubscriber<SampleContainer> subscriber) throws InterruptedException {
        assertThat(subscriber.takeNext(TIMEOUT_DURATION).getStringValue()).isEqualTo("Event1");
        assertThat(subscriber.takeNext(TIMEOUT_DURATION).getStringValue()).isEqualTo("Event2");
        assertThat(subscriber.takeNext(TIMEOUT_DURATION).getStringValue()).isEqualTo("Event3");
        assertThat(subscriber.awaitClosed(TIMEOUT_DURATION)).isTrue();
        assertThat(subscriber.hasError()).isFalse();
    }

    @Test(timeout = 30_000)
    public void testMonoCancel() {
        sampleService.block = true;
        testCancellation(client.getOneValue().subscribe());
    }

    @Test(timeout = 30_000)
    public void testFluxStreamCancel() {
        sampleService.block = true;
        testCancellation(client.stream().subscribe());
    }

    private void testCancellation(Disposable disposable) {
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> sampleService.requestTimestamp > 0);
        disposable.dispose();
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> sampleService.cancelled);
    }

    private interface SampleServiceReactApi {

        Mono<SampleContainer> getOneValue();

        Mono<SampleContainer> getOneValue(CallMetadata callMetadata);

        Mono<Void> setOneValue(SampleContainer value);

        Mono<Void> setOneValue(SampleContainer value, CallMetadata callMetadata);

        Flux<SampleContainer> stream();

        Flux<SampleContainer> stream(CallMetadata callMetadata);
    }

    private class SampleServiceImpl extends SampleServiceGrpc.SampleServiceImplBase {

        private final SimpleGrpcCallMetadataResolver metadataResolver = new SimpleGrpcCallMetadataResolver();

        private long requestTimestamp;
        private SampleContainer lastSet;

        private String expectedCallerId = AnonymousCallMetadataResolver.getInstance().resolve().get().getCallerId();
        private boolean block;
        private volatile boolean cancelled;

        @Override
        public void getOneValue(Empty request, StreamObserver<SampleContainer> responseObserver) {
            onRequest(responseObserver);
            if (!block) {
                responseObserver.onNext(HELLO);
            }
        }

        @Override
        public void setOneValue(SampleContainer request, StreamObserver<Empty> responseObserver) {
            onRequest(responseObserver);
            if (!block) {
                this.lastSet = request;
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
            }
        }

        @Override
        public void stream(Empty request, StreamObserver<SampleContainer> responseObserver) {
            onRequest(responseObserver);
            if (!block) {
                for (int i = 1; i <= 3; i++) {
                    responseObserver.onNext(SampleContainer.newBuilder().setStringValue("Event" + i).build());
                }
                responseObserver.onCompleted();
            }
        }

        private void onRequest(StreamObserver<?> responseObserver) {
            registerCancellationCallback(responseObserver);
            checkMetadata();
        }

        private void registerCancellationCallback(StreamObserver<?> responseObserver) {
            ServerCallStreamObserver serverCallStreamObserver = (ServerCallStreamObserver) responseObserver;
            serverCallStreamObserver.setOnCancelHandler(() -> cancelled = true);
        }

        private void checkMetadata() {
            requestTimestamp = System.currentTimeMillis();
            CallMetadata metadata = metadataResolver.resolve().orElseThrow(() -> new IllegalStateException("no metadata found"));
            assertThat(metadata.getCallerId()).isEqualTo(expectedCallerId);
        }
    }
}