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

package com.netflix.titus.common.network.reverseproxy.grpc;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import com.google.protobuf.Empty;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.testing.SampleGrpcService.SampleContainer;
import com.netflix.titus.testing.SampleServiceGrpc;
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

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteHandlerRegistryTest {

    private static final int ITEMS_IN_STREAM = 3;
    private static final String HELLO = "HELLO";

    private int serverPort;
    private int proxyPort;

    private Server server;
    private Server proxy;

    private ManagedChannel serverChannel;
    private ManagedChannel proxyChannel;

    @Before
    public void setUp() throws Exception {
        this.server = newServer();
        this.serverChannel = newServerClient();
        this.proxy = newProxy();
        this.proxyChannel = newProxyClient();
    }

    @After
    public void tearDown() {
        Evaluators.acceptNotNull(serverChannel, ManagedChannel::shutdownNow);
        Evaluators.acceptNotNull(server, Server::shutdownNow);
        Evaluators.acceptNotNull(proxyChannel, ManagedChannel::shutdownNow);
        Evaluators.acceptNotNull(proxy, Server::shutdownNow);
    }

    @Test
    public void testUnaryCall() {
        SampleContainer result = SampleServiceGrpc.newBlockingStub(proxyChannel).getOneValue(Empty.getDefaultInstance());
        assertThat(result).isNotNull();
        assertThat(result.getStringValue()).isEqualTo(HELLO);
    }

    @Test
    public void testStreamingCall() {
        Iterator<SampleContainer> resultIt = SampleServiceGrpc.newBlockingStub(proxyChannel).stream(Empty.getDefaultInstance());
        int count = 0;
        while (resultIt.hasNext()) {
            assertThat(resultIt.next().getStringValue()).isEqualTo(Integer.toString(count));
            count++;
        }
        assertThat(count).isEqualTo(ITEMS_IN_STREAM);
    }

    private Server newServer() throws IOException {
        Server server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(
                        new SampleServiceGrpc.SampleServiceImplBase() {
                            @Override
                            public void getOneValue(Empty request, StreamObserver<SampleContainer> responseObserver) {
                                responseObserver.onNext(SampleContainer.newBuilder()
                                        .setStringValue(HELLO)
                                        .build()
                                );
                                responseObserver.onCompleted();
                            }

                            @Override
                            public void stream(Empty request, StreamObserver<SampleContainer> responseObserver) {
                                Evaluators.times(
                                        ITEMS_IN_STREAM,
                                        i -> responseObserver.onNext(SampleContainer.newBuilder()
                                                .setStringValue(Integer.toString(i))
                                                .build()
                                        ));
                                responseObserver.onCompleted();
                            }
                        }
                ))
                .build()
                .start();
        this.serverPort = server.getPort();
        return server;
    }

    private ManagedChannel newServerClient() {
        return NettyChannelBuilder.forTarget("localhost:" + serverPort)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
    }

    private Server newProxy() throws IOException {
        RemoteHandlerRegistry registry = new RemoteHandlerRegistry(n -> Optional.of(serverChannel));

        Server server = NettyServerBuilder.forPort(0)
                .fallbackHandlerRegistry(registry)
                .build()
                .start();

        this.proxyPort = server.getPort();

        return server;
    }

    private ManagedChannel newProxyClient() {
        return NettyChannelBuilder.forTarget("localhost:" + proxyPort)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
    }
}