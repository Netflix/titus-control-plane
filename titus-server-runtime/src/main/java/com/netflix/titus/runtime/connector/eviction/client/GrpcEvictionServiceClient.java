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

package com.netflix.titus.runtime.connector.eviction.client;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.grpc.protogen.EvictionServiceEvent;
import com.netflix.titus.grpc.protogen.EvictionServiceGrpc.EvictionServiceStub;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.grpc.protogen.TaskTerminateRequest;
import com.netflix.titus.grpc.protogen.TaskTerminateResponse;
import com.netflix.titus.runtime.connector.eviction.EvictionServiceClient;
import com.netflix.titus.runtime.endpoint.common.grpc.ReactorGrpcClientAdapter;
import com.netflix.titus.runtime.endpoint.common.grpc.ReactorGrpcClientAdapterFactory;
import com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters.toGrpcReference;

@Singleton
public class GrpcEvictionServiceClient implements EvictionServiceClient {

    private final ReactorGrpcClientAdapter<EvictionServiceStub> grpcClientAdapter;

    @Inject
    public GrpcEvictionServiceClient(ReactorGrpcClientAdapterFactory grpcClientAdapterFactory,
                                     EvictionServiceStub client) {
        this.grpcClientAdapter = grpcClientAdapterFactory.newAdapter(client);
    }

    @Override
    public Mono<EvictionQuota> getEvictionQuota(Reference reference) {
        return grpcClientAdapter.<com.netflix.titus.grpc.protogen.EvictionQuota>asMono((client, streamObserver) ->
                client.getEvictionQuota(toGrpcReference(reference), streamObserver)
        ).map(GrpcEvictionModelConverters::toCoreEvictionQuota);
    }

    @Override
    public Mono<Void> terminateTask(String taskId, String reason) {
        TaskTerminateRequest request = TaskTerminateRequest.newBuilder()
                .setTaskId(taskId)
                .setReason(reason)
                .build();

        return grpcClientAdapter.<TaskTerminateResponse>asMono((client, streamObserver) ->
                client.terminateTask(request, streamObserver)
        ).flatMap(response -> {
            if (!response.getAllowed()) {
                // TODO Better error handling
                return Mono.error(new IllegalStateException(response.getReasonCode() + ": " + response.getReasonMessage()));
            }
            return Mono.empty();
        });
    }

    @Override
    public Flux<EvictionEvent> observeEvents(boolean includeSnapshot) {

        ObserverEventRequest request = ObserverEventRequest.newBuilder()
                .setIncludeSnapshot(includeSnapshot)
                .build();

        return grpcClientAdapter.<EvictionServiceEvent>asFlux((client, streamObserver) -> {
            client.observeEvents(request, streamObserver);
        }).map(GrpcEvictionModelConverters::toCoreEvent);
    }
}
