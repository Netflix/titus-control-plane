package com.netflix.titus.common.network.reverseproxy.grpc;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import com.google.protobuf.Empty;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.testing.SampleGrpcService.SampleContainer;
import com.netflix.titus.testing.SampleServiceGrpc;
import com.netflix.titus.testkit.util.NetworkExt;
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

    private final int serverPort = NetworkExt.findUnusedPort();
    private final int proxyPort = NetworkExt.findUnusedPort();

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
        return NettyServerBuilder.forPort(serverPort)
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
    }

    private ManagedChannel newServerClient() {
        return NettyChannelBuilder.forTarget("localhost:" + serverPort)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
    }

    private Server newProxy() throws IOException {
        RemoteHandlerRegistry registry = new RemoteHandlerRegistry(n -> Optional.of(serverChannel));

        return NettyServerBuilder.forPort(proxyPort)
                .fallbackHandlerRegistry(registry)
                .build()
                .start();
    }

    private ManagedChannel newProxyClient() {
        return NettyChannelBuilder.forTarget("localhost:" + proxyPort)
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();
    }
}