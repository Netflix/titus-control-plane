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

package com.netflix.titus.runtime.endpoint.common.grpc;

import java.io.IOException;
import java.net.SocketException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.fit.adapter.GrpcFitInterceptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.runtime.endpoint.common.grpc.interceptor.CommonErrorCatchingServerInterceptor;
import com.netflix.titus.runtime.endpoint.metadata.V3HeaderInterceptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TitusGrpcServer {

    private static final Logger logger = LoggerFactory.getLogger(TitusGrpcServer.class);

    private static final Duration DEFAULT_SHUTDOWN_TIME = Duration.ofSeconds(5);

    private final Duration shutdownTime;

    private final Server server;
    private final AtomicBoolean started = new AtomicBoolean();

    private TitusGrpcServer(Server server,
                            Duration shutdownTime) {
        this.server = server;
        this.shutdownTime = Evaluators.getOrDefault(shutdownTime, DEFAULT_SHUTDOWN_TIME);
    }

    public int getPort() {
        return server.getPort();
    }

    @PostConstruct
    public void start() {
        if (!started.getAndSet(true)) {
            logger.info("Starting GRPC server {}...", getClass().getSimpleName());
            try {
                this.server.start();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            logger.info("Started {} on port {}.", getClass().getSimpleName(), getPort());
        }
    }

    @PreDestroy
    public void shutdown() {
        if (!server.isShutdown()) {
            if (shutdownTime.toMillis() <= 0) {
                server.shutdownNow();
            } else {
                server.shutdown();
                try {
                    server.awaitTermination(shutdownTime.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignore) {
                }
                if (!server.isShutdown()) {
                    server.shutdownNow();
                }
            }
        }
    }

    public static Builder newBuilder(int port, TitusRuntime titusRuntime) {
        return new Builder().withPort(port).withTitusRuntime(titusRuntime);
    }

    public static class Builder {

        private int port;
        private TitusRuntime titusRuntime;

        private final Map<String, ServiceBuilder> serviceBuilders = new HashMap<>();
        private final List<ServerInterceptor> interceptors = new ArrayList<>();
        private final List<Function<Throwable, Optional<Status>>> serviceExceptionMappers = new ArrayList<>();
        private Duration shutdownTime;
        private final List<UnaryOperator<ServerBuilder>> serverConfigurers = new ArrayList<>();

        private Builder() {
            // Add default exception mappings.
            serviceExceptionMappers.add(cause -> {
                if (cause instanceof SocketException) {
                    return Optional.of(Status.UNAVAILABLE);
                } else if (cause instanceof TimeoutException) {
                    return Optional.of(Status.DEADLINE_EXCEEDED);
                }
                return Optional.empty();
            });
        }

        public Builder withPort(int port) {
            this.port = port;
            return this;
        }

        public Builder withTitusRuntime(TitusRuntime titusRuntime) {
            this.titusRuntime = titusRuntime;
            return this;
        }

        public Builder withShutdownTime(Duration shutdownTime) {
            this.shutdownTime = shutdownTime;
            return this;
        }

        public Builder withServerConfigurer(UnaryOperator<ServerBuilder> serverConfigurer) {
            this.serverConfigurers.add(serverConfigurer);
            return this;
        }

        public Builder withCallMetadataInterceptor() {
            interceptors.add(new V3HeaderInterceptor());
            return this;
        }

        public Builder withExceptionMapper(Function<Throwable, Optional<Status>> serviceExceptionMapper) {
            serviceExceptionMappers.add(serviceExceptionMapper);
            return this;
        }

        public Builder withInterceptor(ServerInterceptor serverInterceptor) {
            interceptors.add(serverInterceptor);
            return this;
        }

        public Builder withService(ServerServiceDefinition serviceDefinition, List<ServerInterceptor> interceptors) {
            serviceBuilders.put(serviceDefinition.getServiceDescriptor().getName(), new ServiceBuilder(serviceDefinition, interceptors));
            return this;
        }

        public TitusGrpcServer build() {
            Preconditions.checkArgument(port >= 0, "Port number is negative");
            Preconditions.checkNotNull(titusRuntime, "TitusRuntime not set");

            List<ServerInterceptor> commonInterceptors = new ArrayList<>();
            commonInterceptors.add(new CommonErrorCatchingServerInterceptor(new GrpcExceptionMapper(serviceExceptionMappers)));
            GrpcFitInterceptor.getIfFitEnabled(titusRuntime).ifPresent(commonInterceptors::add);
            commonInterceptors.addAll(interceptors);

            ServerBuilder serverBuilder = ServerBuilder.forPort(port);
            for (UnaryOperator<ServerBuilder> c : serverConfigurers) {
                c.apply(serverBuilder);
            }
            for (ServiceBuilder serviceBuilder : serviceBuilders.values()) {
                serverBuilder.addService(serviceBuilder.build(commonInterceptors));
            }

            return new TitusGrpcServer(serverBuilder.build(), shutdownTime);
        }
    }

    private static class ServiceBuilder {

        private final ServerServiceDefinition serviceDefinition;
        private final List<ServerInterceptor> interceptors;

        private ServiceBuilder(ServerServiceDefinition serviceDefinition, List<ServerInterceptor> interceptors) {
            this.serviceDefinition = serviceDefinition;
            this.interceptors = interceptors;
        }

        private ServerServiceDefinition build(List<ServerInterceptor> commonInterceptors) {
            return ServerInterceptors.intercept(
                    serviceDefinition,
                    CollectionsExt.merge(commonInterceptors, interceptors)
            );
        }
    }
}
