/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.common.util.grpc.reactor.server;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.grpc.reactor.SampleContext;
import com.netflix.titus.common.util.grpc.reactor.SampleContextServerInterceptor;
import com.netflix.titus.common.util.grpc.reactor.SampleServiceReactorClient;
import com.netflix.titus.common.util.grpc.reactor.client.ReactorToGrpcClientBuilder;
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
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class GrpcToReactorServerBuilderTest {

    private static final long TIMEOUT_MS = 30_000;

    private static final Duration TIMEOUT_DURATION = Duration.ofMillis(TIMEOUT_MS);

    private static final SampleContainer HELLO = SampleContainer.newBuilder().setStringValue("Hello").build();

    private static final SampleContext JUNIT_CONTEXT = new SampleContext("junitContext");

    private ReactorSampleServiceImpl reactorSampleService;

    private ManagedChannel channel;
    private Server server;

    private SampleServiceReactorClient client;

    @Before
    public void setUp() throws Exception {
        createReactorGrpcServer(new ReactorSampleServiceImpl());
    }

    @After
    public void tearDown() {
        ExceptionExt.silent(channel, ManagedChannel::shutdownNow);
        ExceptionExt.silent(server, Server::shutdownNow);
    }

    private void createReactorGrpcServer(ReactorSampleServiceImpl reactorSampleService) throws Exception {
        this.reactorSampleService = reactorSampleService;

        DefaultGrpcToReactorServerFactory<SampleContext> factory = new DefaultGrpcToReactorServerFactory<>(SampleContext.class, SampleContextServerInterceptor::serverResolve);
        ServerServiceDefinition serviceDefinition = factory.apply(SampleServiceGrpc.getServiceDescriptor(), reactorSampleService, ReactorSampleServiceImpl.class);

        this.server = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(serviceDefinition, new SampleContextServerInterceptor()))
                .build()
                .start();

        this.channel = NettyChannelBuilder.forTarget("localhost:" + server.getPort())
                .negotiationType(NegotiationType.PLAINTEXT)
                .build();

        this.client = ReactorToGrpcClientBuilder.newBuilder(SampleServiceReactorClient.class, SampleServiceGrpc.newStub(channel), SampleServiceGrpc.getServiceDescriptor(), SampleContext.class)
                .withTimeout(TIMEOUT_DURATION)
                .withStreamingTimeout(Duration.ofMillis(ReactorToGrpcClientBuilder.DEFAULT_STREAMING_TIMEOUT_MS))
                .withGrpcStubDecorator(SampleContextServerInterceptor::attachClientContext)
                .build();
    }

    @Test(timeout = 30_000)
    public void testMonoGet() {
        assertThat(client.getOneValue().block().getStringValue()).isEqualTo("Hello");
    }

    @Test(timeout = 30_000)
    public void testMonoGetWithCallMetadata() {
        reactorSampleService.expectedContext = JUNIT_CONTEXT;
        assertThat(client.getOneValue(JUNIT_CONTEXT).block().getStringValue()).isEqualTo("Hello");
    }

    @Test(timeout = 30_000)
    public void testMonoSet() {
        assertThat(client.setOneValue(HELLO).block()).isNull();
        assertThat(reactorSampleService.lastSet).isEqualTo(HELLO);
    }

    @Test(timeout = 30_000)
    public void testMonoSetWithCallMetadata() {
        reactorSampleService.expectedContext = JUNIT_CONTEXT;
        assertThat(client.setOneValue(HELLO, JUNIT_CONTEXT).block()).isNull();
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
        reactorSampleService.expectedContext = JUNIT_CONTEXT;
        TitusRxSubscriber<SampleContainer> subscriber = new TitusRxSubscriber<>();

        client.stream(JUNIT_CONTEXT).subscribe(subscriber);
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

    @Test(timeout = 30_000)
    public void testAopCglibProxy() throws Exception {
        // Tear down the non-proxied server
        tearDown();

        // Create proxied version of the service
        ReactorSampleServiceImpl reactorSampleServiceToProxy = new ReactorSampleServiceImpl();
        ProxyFactory proxyFactory = new ProxyFactory(reactorSampleServiceToProxy);
        ReactorSampleServiceImpl proxy = (ReactorSampleServiceImpl) proxyFactory.getProxy();
        assertThat(AopUtils.isCglibProxy(proxy)).isTrue();

        // Create server with proxied service
        createReactorGrpcServer(proxy);

        assertThat(client.setOneValue(HELLO).block()).isNull();
        assertThat(reactorSampleServiceToProxy.lastSet).isEqualTo(HELLO);
    }

    private void testCancellation(Disposable disposable) {
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> reactorSampleService.requestTimestamp > 0);
        disposable.dispose();
        await().timeout(TIMEOUT_MS, TimeUnit.MILLISECONDS).until(() -> reactorSampleService.cancelled);
    }

    public static class ReactorSampleServiceImpl {

        private long requestTimestamp;
        private SampleContainer lastSet;

        private volatile SampleContext expectedContext = SampleContextServerInterceptor.CONTEXT_UNDEFINED;
        private boolean block;
        private volatile boolean cancelled;

        public Mono<SampleContainer> getOneValue(SampleContext context) {
            checkContext(context);
            return block ? Mono.<SampleContainer>never().doOnCancel(() -> cancelled = true) : Mono.just(HELLO);
        }

        public Mono<Void> setOneValue(SampleContainer request, SampleContext context) {
            checkContext(context);
            this.lastSet = request;
            return block ? Mono.<Void>never().doOnCancel(() -> cancelled = true) : Mono.empty();
        }

        public Flux<SampleContainer> stream(SampleContext context) {
            checkContext(context);
            if (block) {
                return Flux.<SampleContainer>never().doOnCancel(() -> cancelled = true);
            }
            Stream<SampleContainer> events = IntStream.of(1, 2, 3).mapToObj(i ->
                    SampleContainer.newBuilder().setStringValue("Event" + i).build()
            );
            return Flux.fromStream(events);
        }

        private void checkContext(SampleContext context) {
            requestTimestamp = System.currentTimeMillis();
            assertThat(context).isEqualTo(expectedContext);
        }
    }
}