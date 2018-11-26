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

package com.netflix.titus.runtime.connector.common.react;

import java.time.Duration;

import com.google.protobuf.Empty;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.endpoint.metadata.AnonymousCallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadata;
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
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactToGrpcClientBuilderTest {

    private ManagedChannel channel;
    private Server server;
    private SampleServiceReactApi client;

    @Before
    public void setUp() throws Exception {
        this.server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(new SampleServiceImpl(), new V3HeaderInterceptor()))
                .build()
                .start();
        this.channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();

        this.client = ReactToGrpcClientBuilder.newBuilder(SampleServiceReactApi.class, SampleServiceGrpc.newStub(channel), SampleServiceGrpc.getServiceDescriptor())
                .withTimeout(Duration.ofSeconds(30))
                .withCallMetadataResolver(new AnonymousCallMetadataResolver())
                .build();
    }

    @After
    public void tearDown() {
        ExceptionExt.silent(channel, ManagedChannel::shutdownNow);
        ExceptionExt.silent(server, Server::shutdownNow);
    }

    @Test(timeout = 30_000)
    public void testMono() {
        assertThat(client.getOneValue().block().getStringValue()).isEqualTo("Hello");
    }

    @Test(timeout = 30_000)
    public void testFlux() throws InterruptedException {
        TitusRxSubscriber<SampleContainer> subscriber = new TitusRxSubscriber<>();
        client.stream().subscribe(subscriber);

        assertThat(subscriber.takeNext(Duration.ofSeconds(1)).getStringValue()).isEqualTo("Event1");
        assertThat(subscriber.takeNext(Duration.ofSeconds(1)).getStringValue()).isEqualTo("Event2");
        assertThat(subscriber.takeNext(Duration.ofSeconds(1)).getStringValue()).isEqualTo("Event3");
        assertThat(subscriber.awaitClosed(Duration.ofSeconds(1))).isTrue();
        assertThat(subscriber.hasError()).isFalse();
    }

    private interface SampleServiceReactApi {

        Mono<SampleContainer> getOneValue();

        Flux<SampleContainer> stream();
    }

    private class SampleServiceImpl extends SampleServiceGrpc.SampleServiceImplBase {

        private final SimpleGrpcCallMetadataResolver metadataResolver = new SimpleGrpcCallMetadataResolver();

        @Override
        public void getOneValue(Empty request, StreamObserver<SampleContainer> responseObserver) {
            checkMetadata();
            responseObserver.onNext(SampleContainer.newBuilder()
                    .setStringValue("Hello")
                    .build()
            );
        }

        @Override
        public void stream(Empty request, StreamObserver<SampleContainer> responseObserver) {
            checkMetadata();
            for (int i = 1; i <= 3; i++) {
                responseObserver.onNext(SampleContainer.newBuilder().setStringValue("Event" + i).build());
            }
            responseObserver.onCompleted();
        }

        private void checkMetadata() {
            CallMetadata metadata = metadataResolver.resolve().orElseThrow(() -> new IllegalStateException("no metadata found"));
            assertThat(metadata.getCallerId()).isEqualTo("anonymous");
        }
    }
}