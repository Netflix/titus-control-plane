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

package com.netflix.titus.runtime.connector.eviction;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.grpc.protogen.ObserverEventRequest;
import com.netflix.titus.grpc.protogen.TaskTerminateRequest;
import com.netflix.titus.runtime.eviction.endpoint.grpc.GrpcEvictionModelConverters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Singleton
public class RemoteEvictionServiceClient implements EvictionServiceClient {

    private final ReactorEvictionServiceStub stub;

    @Inject
    public RemoteEvictionServiceClient(ReactorEvictionServiceStub stub) {
        this.stub = stub;
    }

    @Override
    public Mono<EvictionQuota> getEvictionQuota(Reference reference) {
        return stub.getEvictionQuota(GrpcEvictionModelConverters.toGrpcReference(reference)).map(GrpcEvictionModelConverters::toCoreEvictionQuota);
    }

    @Override
    public Mono<Void> terminateTask(String taskId, String reason) {
        return stub.terminateTask(TaskTerminateRequest.newBuilder()
                .setTaskId(taskId)
                .setReason(reason)
                .build()
        ).flatMap(response -> {
            if (response.getAllowed()) {
                return Mono.empty();
            }
            return Mono.error(EvictionException.deconstruct(response.getReasonCode(), response.getReasonMessage()));
        });
    }

    @Override
    public Flux<EvictionEvent> observeEvents(boolean includeSnapshot) {
        return stub.observeEvents(ObserverEventRequest.newBuilder()
                .setIncludeSnapshot(includeSnapshot)
                .build()
        ).map(GrpcEvictionModelConverters::toCoreEvent);
    }
}
