package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.protobuf.Empty;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.Deadline;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultGrpcClientCallAssistant<STUB extends AbstractStub<STUB>> implements GrpcClientCallAssistant<STUB> {

    private final GrpcCallAssistantConfiguration configuration;
    private final STUB stub;
    private final CallMetadataResolver callMetadataResolver;

    public DefaultGrpcClientCallAssistant(GrpcCallAssistantConfiguration configuration,
                                          STUB stub,
                                          CallMetadataResolver callMetadataResolver) {
        this.configuration = configuration;
        this.stub = stub;
        this.callMetadataResolver = callMetadataResolver;
    }

    @Override
    public <T> Mono<T> asMono(BiConsumer<STUB, StreamObserver<T>> grpcInvoker) {
        return Mono.create(sink -> {
            STUB decoratedStub = decorateStub(configuration.getRequestTimeoutMs());
            StreamObserver<T> grpcStreamObserver = new ClientResponseObserver<T, T>() {
                @Override
                public void beforeStart(ClientCallStreamObserver requestStream) {
                    sink.onCancel(() -> requestStream.cancel("React subscription cancelled", null));
                }

                @Override
                public void onNext(T value) {
                    sink.success(value);
                }

                @Override
                public void onError(Throwable error) {
                    sink.error(error);
                }

                @Override
                public void onCompleted() {
                    sink.success();
                }
            };

            grpcInvoker.accept(decoratedStub, grpcStreamObserver);
        });
    }

    @Override
    public Mono<Void> asMonoEmpty(BiConsumer<STUB, StreamObserver<Empty>> grpcInvoker) {
        return Mono.create(sink -> {
            STUB decoratedStub = decorateStub(configuration.getRequestTimeoutMs());
            StreamObserver<Empty> grpcStreamObserver = new ClientResponseObserver<Empty, Empty>() {
                @Override
                public void beforeStart(ClientCallStreamObserver requestStream) {
                    sink.onCancel(() -> requestStream.cancel("React subscription cancelled", null));
                }

                @Override
                public void onNext(Empty value) {
                    sink.success();
                }

                @Override
                public void onError(Throwable error) {
                    sink.error(error);
                }

                @Override
                public void onCompleted() {
                    sink.success();
                }
            };

            grpcInvoker.accept(decoratedStub, grpcStreamObserver);
        });
    }

    @Override
    public <T> Flux<T> asFlux(BiConsumer<STUB, StreamObserver<T>> grpcInvoker) {
        return Flux.create(sink -> {
            STUB decoratedStub = decorateStub(configuration.getStreamTimeoutMs());
            StreamObserver<T> grpcStreamObserver = new ClientResponseObserver<T, T>() {
                @Override
                public void beforeStart(ClientCallStreamObserver requestStream) {
                    sink.onCancel(() -> requestStream.cancel("React subscription cancelled", null));
                }

                @Override
                public void onNext(T value) {
                    sink.next(value);
                }

                @Override
                public void onError(Throwable error) {
                    sink.error(error);
                }

                @Override
                public void onCompleted() {
                    sink.complete();
                }
            };

            grpcInvoker.accept(decoratedStub, grpcStreamObserver);
        });
    }

    @Override
    public <T> Flux<T> callFlux(Function<STUB, Flux<T>> grpcExecutor) {
        return Flux.defer(() -> {
            STUB decoratedStub = decorateStub(configuration.getStreamTimeoutMs());
            return grpcExecutor.apply(decoratedStub);
        });
    }

    @Override
    public void inContext(Consumer<STUB> grpcExecutor) {
        STUB decoratedStub = decorateStub(configuration.getStreamTimeoutMs());
        grpcExecutor.accept(decoratedStub);
    }

    private STUB decorateStub(long timeoutMs) {
        STUB decoratedStub = callMetadataResolver.resolve().map(callMetadata ->
                V3HeaderInterceptor.attachCallMetadata(stub, callMetadata)
        ).orElse(stub);
        decoratedStub = decoratedStub.withDeadline(Deadline.after(timeoutMs, TimeUnit.MILLISECONDS));
        return decoratedStub;
    }
}
