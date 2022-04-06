package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.protobuf.Empty;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface GrpcClientCallAssistant<STUB extends AbstractStub<STUB>> {

    <T> Mono<T> asMono(BiConsumer<STUB, StreamObserver<T>> grpcInvoker);

    Mono<Void> asMonoEmpty(BiConsumer<STUB, StreamObserver<Empty>> grpcInvoker);

    <T> Flux<T> asFlux(BiConsumer<STUB, StreamObserver<T>> grpcInvoker);

    <T> Flux<T> callFlux(Function<STUB, Flux<T>> grpcExecutor);

    void inContext(Consumer<STUB> grpcExecutor);
}
