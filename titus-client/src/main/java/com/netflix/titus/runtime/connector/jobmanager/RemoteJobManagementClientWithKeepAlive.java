/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.jobmanager;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.FunctionExt;
import com.netflix.titus.grpc.protogen.JobChangeNotification;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import com.netflix.titus.grpc.protogen.KeepAliveRequest;
import com.netflix.titus.grpc.protogen.ObserveJobsQuery;
import com.netflix.titus.grpc.protogen.ObserveJobsWithKeepAliveRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Extension of {@link RemoteJobManagementClient} that uses the new observeJobsWithKeepAlive GRPC method.
 * Its usage is limited now to communication between TitusGateway and TJC.
 */
public class RemoteJobManagementClientWithKeepAlive extends RemoteJobManagementClient {
    private static final Logger logger = LoggerFactory.getLogger(RemoteJobManagementClientWithKeepAlive.class);

    private final JobConnectorConfiguration configuration;

    /**
     * Reactor bridge does not support input streams, so we have to use the GRPC stub directly here.
     */
    private final JobManagementServiceStub stub;

    private final TitusRuntime titusRuntime;

    private final AtomicLong keepAliveIdGen = new AtomicLong();

    public RemoteJobManagementClientWithKeepAlive(String clientName,
                                                  JobConnectorConfiguration configuration,
                                                  JobManagementServiceStub stub,
                                                  ReactorJobManagementServiceStub reactorStub,
                                                  TitusRuntime titusRuntime) {
        super(clientName, reactorStub, titusRuntime);
        this.configuration = configuration;
        this.stub = stub;
        this.titusRuntime = titusRuntime;
    }

    /**
     * Only used for unit testing to ensure internal subscriptions are not leaked.
     * The <t>keepAliveCompleted</t> callback runs when the keep alive (interval) subscription is disposed
     */
    @VisibleForTesting
    Flux<JobChangeNotification> connectObserveJobs(Map<String, String> filteringCriteria, Runnable keepAliveCompleted) {
        return Flux.create(sink -> {
            AtomicReference<ClientCallStreamObserver> requestStreamRef = new AtomicReference<>();
            StreamObserver<JobChangeNotification> grpcStreamObserver = new ClientResponseObserver<JobChangeNotification, JobChangeNotification>() {
                @Override
                public void beforeStart(ClientCallStreamObserver requestStream) {
                    requestStreamRef.set(requestStream);
                }

                @Override
                public void onNext(JobChangeNotification value) {
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

            StreamObserver<ObserveJobsWithKeepAliveRequest> clientStreamObserver = stub.observeJobsWithKeepAlive(grpcStreamObserver);
            clientStreamObserver.onNext(ObserveJobsWithKeepAliveRequest.newBuilder()
                    .setQuery(ObserveJobsQuery.newBuilder().putAllFilteringCriteria(filteringCriteria).build())
                    .build()
            );

            // Now emit keep alive requests periodically
            Disposable keepAliveSubscription = Flux.interval(Duration.ofMillis(configuration.getKeepAliveIntervalMs()))
                    // doOnCancel is confusing: it's called when the subscription is disposed
                    // It should be named doOnDispose. See: https://github.com/reactor/reactor-core/issues/1240
                    .doOnCancel(
                            () -> ExceptionExt.doCatch(keepAliveCompleted)
                                    .ifPresent(t -> logger.warn("Error running the keepAliveCompleted callback", t))
                    )
                    .subscribe(
                            next -> {
                                try {
                                    clientStreamObserver.onNext(ObserveJobsWithKeepAliveRequest.newBuilder()
                                            .setKeepAliveRequest(KeepAliveRequest.newBuilder()
                                                    .setRequestId(keepAliveIdGen.getAndIncrement())
                                                    .setTimestamp(titusRuntime.getClock().wallTime())
                                                    .build()
                                            )
                                            .build()
                                    );
                                } catch (Exception error) {
                                    clientStreamObserver.onError(error);
                                }
                            },
                            sink::error,
                            () -> sink.error(new IllegalArgumentException("Keep alive stream terminated. Closing the event stream"))
                    );

            sink.onDispose(() -> {
                keepAliveSubscription.dispose();
                if (requestStreamRef.get() != null) {
                    requestStreamRef.get().cancel("ObserveJobs stream cancelled by the client", null);
                }
            });
        });

    }

    @Override
    protected Flux<JobChangeNotification> connectObserveJobs(Map<String, String> filteringCriteria) {
        return connectObserveJobs(filteringCriteria, FunctionExt.noop());
    }
}
