/*
 * Copyright 2019 Netflix, Inc.
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.connector.GrpcRequestConfiguration;
import com.netflix.titus.runtime.connector.common.reactor.server.DefaultGrpcToReactorServerFactory;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.SimpleGrpcCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import com.netflix.titus.testing.SampleGrpcService.SampleContainer;
import com.netflix.titus.testing.SampleServiceGrpc;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.jayway.awaitility.Awaitility.await;
import static com.netflix.titus.api.jobmanager.service.JobManagerConstants.GRPC_REPLICATOR_CALL_METADATA;
import static org.assertj.core.api.Assertions.assertThat;

public class GrpcToReactorServerBuilderTest {

    private static final long TIMEOUT_MS = 30_000;

    private static final Duration TIMEOUT_DURATION = Duration.ofMillis(TIMEOUT_MS);

    private static final SampleContainer HELLO = SampleContainer.newBuilder().setStringValue("Hello").build();

    private ReactorSampleServiceImpl reactorSampleService;

    private ManagedChannel channel;
    private Server server;

    private SampleServiceReactorClient client;

    @Before
    public void setUp() throws Exception {
        this.reactorSampleService = new ReactorSampleServiceImpl();

        DefaultGrpcToReactorServerFactory factory = new DefaultGrpcToReactorServerFactory(new SimpleGrpcCallMetadataResolver());
        ServerServiceDefinition serviceDefinition = factory.apply(SampleServiceGrpc.getServiceDescriptor(), reactorSampleService);

        this.server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(serviceDefinition, new V3HeaderInterceptor()))
                .build()
                .start();

        this.channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();

        this.client = ReactorToGrpcClientBuilder.newBuilder(SampleServiceReactorClient.class, SampleServiceGrpc.newStub(channel), SampleServiceGrpc.getServiceDescriptor())
                .withTimeout(TIMEOUT_DURATION)
                .withStreamingTimeout(Duration.ofMillis(GrpcRequestConfiguration.DEFAULT_STREAMING_TIMEOUT_MS))
                .withCallMetadataResolver(AnonymousCallMetadataResolver.getInstance())
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
        reactorSampleService.expectedCallerId = GRPC_REPLICATOR_CALL_METADATA.getCallers().get(0).getId();
        assertThat(client.getOneValue(GRPC_REPLICATOR_CALL_METADATA).block().getStringValue()).isEqualTo("Hello");
    }

    @Test(timeout = 30_000)
    public void testMonoSet() {
        assertThat(client.setOneValue(HELLO).block()).isNull();
        assertThat(reactorSampleService.lastSet).isEqualTo(HELLO);
    }

    @Test(timeout = 30_000)
    public void testMonoSetWithCallMetadata() {
        reactorSampleService.expectedCallerId = GRPC_REPLICATOR_CALL_METADATA.getCallerId();
        assertThat(client.setOneValue(HELLO, GRPC_REPLICATOR_CALL_METADATA).block()).isNull();
        assertThat(reactorSampleService.lastSet).isEqualTo(HELLO);
    }

    @Test(timeout = 30_000)
    public void testFlux() throws InterruptedException {
        TitusRxSubscriber<SampleContainer> subscriber = new TitusRxSubscriber<>();
        client.stream().subscribe(subscriber);
        checkTestFlux(subscriber);
    }

    @Test(timeout = 30_000)
    public void testFluxWithCallMetadata() throws InterruptedException {
        reactorSampleService.expectedCallerId = GRPC_REPLICATOR_CALL_METADATA.getCallerId();
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
        reactorSampleService.block = true;
        testCancellation(client.getOneValue().subscribe());
    }

    @Test(timeout = 30_000)
    public void testFluxStreamCancel() {
        reactorSampleService.block = true;
        testCancellation(client.stream().subscribe());
    }

    private void testCancellation(Disposable disposable) {
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> reactorSampleService.requestTimestamp > 0);
        disposable.dispose();
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> reactorSampleService.cancelled);
    }

    public static class ReactorSampleServiceImpl {

        private long requestTimestamp;
        private SampleContainer lastSet;

        private String expectedCallerId = AnonymousCallMetadataResolver.getInstance().resolve().get().getCallers().get(0).getId();
        private boolean block;
        private volatile boolean cancelled;

        public Mono<SampleContainer> getOneValue(CallMetadata callMetadata) {
            checkMetadata(callMetadata);
            return block ? Mono.<SampleContainer>never().doOnCancel(() -> cancelled = true) : Mono.just(HELLO);
        }

        public Mono<Void> setOneValue(SampleContainer request, CallMetadata callMetadata) {
            checkMetadata(callMetadata);
            this.lastSet = request;
            return block ?  Mono.<Void>never().doOnCancel(() -> cancelled = true) : Mono.empty();
        }

        public Flux<SampleContainer> stream(CallMetadata callMetadata) {
            checkMetadata(callMetadata);
            if (block) {
                return Flux.<SampleContainer>never().doOnCancel(() -> cancelled = true);
            }
            Stream<SampleContainer> events = IntStream.of(1, 2, 3).mapToObj(i ->
                    SampleContainer.newBuilder().setStringValue("Event" + i).build()
            );
            return Flux.fromStream(events);
        }

        private void checkMetadata(CallMetadata metadata) {
            requestTimestamp = System.currentTimeMillis();
            assertThat(metadata.getCallers().get(0).getId()).isEqualTo(expectedCallerId);
        }
    }
}