package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface GrpcServerCallAssistant {

    <T> void callMono(StreamObserver<T> streamObserver, Function<GrpcServerCallContext, Mono<T>> monoResponse);

    void callMonoEmpty(StreamObserver<Empty> streamObserver, Function<GrpcServerCallContext, Mono<Void>> monoResponse);

    <T> void callFlux(StreamObserver<T> streamObserver, Function<GrpcServerCallContext, Flux<T>> fluxResponse);

    <T> void callDirect(StreamObserver<T> streamObserver, Function<GrpcServerCallContext, T> responseFunction);

    <T> void inContext(StreamObserver<T> streamObserver,
                       BiConsumer<GrpcServerCallContext, T> onSuccess,
                       BiConsumer<GrpcServerCallContext, Throwable> onError,
                       BiConsumer<GrpcServerCallContext, StreamObserver<T>> context);
}
