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

package com.netflix.titus.common.framework.fit.adapter;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.framework.fit.internal.action.FitLatencyAction;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExceptionExt;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class GrpcFitInterceptor implements ServerInterceptor {

    public static final String COMPONENT = "grpcServer";

    private final Optional<FitInjection> fitRequestHandler;
    private final Scheduler scheduler;

    public GrpcFitInterceptor(TitusRuntime titusRuntime, Scheduler scheduler) {
        this.scheduler = scheduler;
        FitFramework fit = titusRuntime.getFitFramework();
        if (fit.isActive()) {
            FitInjection fitRequestHandler = fit.newFitInjectionBuilder("grpcRequestHandler")
                    .withDescription("GRPC request handler (inject request errors or latencies")
                    .build();
            fit.getRootComponent().getChild(COMPONENT).addInjection(fitRequestHandler);

            this.fitRequestHandler = Optional.of(fitRequestHandler);
        } else {
            this.fitRequestHandler = Optional.empty();
        }
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!fitRequestHandler.isPresent()) {
            return next.startCall(call, headers);
        }

        FitInjection fitInjection = fitRequestHandler.get();
        String injectionPoint = call.getMethodDescriptor().getFullMethodName();

        // Request failure
        try {
            fitInjection.beforeImmediate(injectionPoint);
        } catch (Exception e) {
            call.close(Status.UNAVAILABLE.withDescription("FIT server failure"), new Metadata());
            return new ServerCall.Listener<ReqT>() {
            };
        }

        // Increased latency.
        return fitInjection.findAction(FitLatencyAction.ACTION_ID)
                .map(action -> {
                    int latencyMs = ExceptionExt.doTry(() -> Integer.parseInt(action.getProperties().get("latency"))).orElse(100);
                    return new LatencyHandler<>(call, headers, next, latencyMs).getLatencyListener();
                })
                .orElse(next.startCall(call, headers));
    }

    public static Optional<ServerInterceptor> getIfFitEnabled(TitusRuntime titusRuntime) {
        return titusRuntime.getFitFramework().isActive()
                ? Optional.of(new GrpcFitInterceptor(titusRuntime, Schedulers.parallel()))
                : Optional.empty();
    }

    public static List<ServerInterceptor> appendIfFitEnabled(List<ServerInterceptor> original,
                                                             TitusRuntime titusRuntime) {
        return titusRuntime.getFitFramework().isActive()
                ? CollectionsExt.copyAndAdd(original, new GrpcFitInterceptor(titusRuntime, Schedulers.parallel()))
                : original;
    }

    private class LatencyHandler<ReqT, RespT> {

        private final ServerCall.Listener<ReqT> latencyListener;
        private final AtomicReference<ServerCall.Listener<ReqT>> nextListenerRef = new AtomicReference<>();

        private final BlockingQueue<Consumer<ServerCall.Listener<ReqT>>> events = new LinkedBlockingQueue<>();
        private final AtomicInteger wip = new AtomicInteger(1);
        private final ServerCall<ReqT, RespT> call;

        private LatencyHandler(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next, long latencyMs) {
            this.call = call;

            // Empty action to give priority for the scheduler callback.
            events.offer(listener -> {
            });

            scheduler.schedule(
                    () -> {
                        try {
                            nextListenerRef.set(next.startCall(call, headers));
                            drain();
                        } catch (Exception e) {
                            call.close(Status.INTERNAL.withDescription("Unexpected error during call processing in FIT interceptor: " + e), new Metadata());
                        }
                    },
                    latencyMs,
                    TimeUnit.MILLISECONDS
            );

            this.latencyListener = new ServerCall.Listener<ReqT>() {
                @Override
                public void onMessage(ReqT message) {
                    queueAndDrainIfReady(listener -> listener.onMessage(message));
                }

                @Override
                public void onHalfClose() {
                    queueAndDrainIfReady(ServerCall.Listener::onHalfClose);
                }

                @Override
                public void onCancel() {
                    queueAndDrainIfReady(ServerCall.Listener::onCancel);
                }

                @Override
                public void onComplete() {
                    queueAndDrainIfReady(ServerCall.Listener::onComplete);
                }

                @Override
                public void onReady() {
                    queueAndDrainIfReady(ServerCall.Listener::onReady);
                }
            };
        }

        private void queueAndDrainIfReady(Consumer<ServerCall.Listener<ReqT>> action) {
            events.offer(action);
            if (wip.getAndIncrement() == 0) {
                drain();
            }
        }

        private void drain() {
            do {
                try {
                    events.poll().accept(nextListenerRef.get());
                } catch (Exception e) {
                    call.close(Status.INTERNAL.withDescription("Unexpected error during call processing in FIT interceptor: " + e), new Metadata());
                }
            } while (wip.decrementAndGet() != 0);
        }

        private ServerCall.Listener<ReqT> getLatencyListener() {
            return latencyListener;
        }
    }
}
