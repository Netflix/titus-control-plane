package com.netflix.titus.runtime.endpoint.common.grpc.assistant;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.collect.Iterables;
import com.google.protobuf.Empty;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.api.model.callmetadata.CallMetadataConstants;
import com.netflix.titus.api.model.callmetadata.Caller;
import com.netflix.titus.api.model.callmetadata.CallerType;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataHeaders;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import com.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultGrpcServerCallAssistant implements GrpcServerCallAssistant {

    private final CallMetadataResolver callMetadataResolver;
    private final HostCallerIdResolver hostCallerIdResolver;

    public DefaultGrpcServerCallAssistant(CallMetadataResolver callMetadataResolver,
                                          HostCallerIdResolver hostCallerIdResolver) {
        this.callMetadataResolver = callMetadataResolver;
        this.hostCallerIdResolver = hostCallerIdResolver;
    }

    public <T> void callMono(StreamObserver<T> streamObserver, Function<GrpcServerCallContext, Mono<T>> monoResponse) {
        Disposable disposable = monoResponse.apply(buildContext()).subscribe(
                next -> {
                    try {
                        streamObserver.onNext(next);
                    } catch (Throwable error) {
                        ExceptionExt.silent(() -> streamObserver.onError(error));
                    }
                },
                error -> ExceptionExt.silent(() -> streamObserver.onError(error)),
                () -> ExceptionExt.silent(streamObserver::onCompleted)
        );
        ((ServerCallStreamObserver) streamObserver).setOnCancelHandler(disposable::dispose);
    }

    @Override
    public void callMonoEmpty(StreamObserver<Empty> streamObserver, Function<GrpcServerCallContext, Mono<Void>> monoResponse) {
        Disposable disposable = monoResponse.apply(buildContext()).subscribe(
                next -> {
                    // Nothing
                },
                error -> ExceptionExt.silent(() -> streamObserver.onError(error)),
                () -> {
                    try {
                        streamObserver.onNext(Empty.getDefaultInstance());
                        ExceptionExt.silent(streamObserver::onCompleted);
                    } catch (Exception error) {
                        ExceptionExt.silent(() -> streamObserver.onError(error));
                    }
                }
        );
        ((ServerCallStreamObserver) streamObserver).setOnCancelHandler(disposable::dispose);
    }

    @Override
    public <T> void callFlux(StreamObserver<T> streamObserver, Function<GrpcServerCallContext, Flux<T>> fluxResponse) {
        Disposable disposable = fluxResponse.apply(buildContext()).subscribe(
                next -> {
                    try {
                        streamObserver.onNext(next);
                    } catch (Throwable error) {
                        ExceptionExt.silent(() -> streamObserver.onError(error));
                    }
                },
                error -> ExceptionExt.silent(() -> streamObserver.onError(error)),
                () -> ExceptionExt.silent(streamObserver::onCompleted)
        );
        ((ServerCallStreamObserver) streamObserver).setOnCancelHandler(disposable::dispose);
    }

    @Override
    public <T> void callDirect(StreamObserver<T> streamObserver, Function<GrpcServerCallContext, T> responseFunction) {
        try {
            T result = responseFunction.apply(buildContext());
            streamObserver.onNext(result);
            ExceptionExt.silent(streamObserver::onCompleted);
        } catch (Exception error) {
            ExceptionExt.silent(() -> streamObserver.onError(error));
        }
    }

    @Override
    public <T> void inContext(StreamObserver<T> streamObserver,
                              BiConsumer<GrpcServerCallContext, T> onSuccess,
                              BiConsumer<GrpcServerCallContext, Throwable> onError,
                              BiConsumer<GrpcServerCallContext, StreamObserver<T>> consumer) {
        GrpcServerCallContext context = buildContext();
        consumer.accept(context, new StreamObserver<T>() {
            @Override
            public void onNext(T next) {
                streamObserver.onNext(next);
                onSuccess.accept(context, next);
            }

            @Override
            public void onError(Throwable throwable) {
                streamObserver.onError(throwable);
                onError.accept(context, throwable);
            }

            @Override
            public void onCompleted() {
                streamObserver.onCompleted();
            }
        });
    }

    private GrpcServerCallContext buildContext() {
        CallMetadata callMetadata = callMetadataResolver.resolve().orElse(null);

        Map<String, String> context = V3HeaderInterceptor.CALLER_CONTEXT_CONTEXT_KEY.get();
        String callerAddress = context == null ? null : context.get(CallMetadataHeaders.DIRECT_CALLER_CONTEXT_CALLER_ADDRESS);

        String callerApplicationName;
        if (callMetadata == null) {
            callerApplicationName = callerAddress == null ? "unknown" : hostCallerIdResolver.resolve(callerAddress).orElse("unknown");
        } else {
            Caller directCaller = Iterables.getLast(callMetadata.getCallers());
            callerApplicationName = directCaller.getCallerType() == CallerType.Application
                    ? "app#" + directCaller.getId()
                    : "user#" + directCaller.getId();
        }

        return new GrpcServerCallContext(
                callMetadataResolver.resolve().orElse(CallMetadataConstants.UNDEFINED_CALL_METADATA),
                Evaluators.getOrDefault(callerAddress, "unknown"),
                callerApplicationName
        );
    }
}
