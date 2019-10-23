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

package com.netflix.titus.runtime.machine;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.v4.Id;
import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineQueryResult;
import com.netflix.titus.grpc.protogen.v4.QueryRequest;
import com.netflix.titus.runtime.connector.machine.ReactorMachineServiceStub;
import reactor.core.publisher.Mono;

@Singleton
public class ReactorGatewayMachineGrpcService {

    private final ReactorMachineServiceStub machineServiceStub;

    @Inject
    public ReactorGatewayMachineGrpcService(ReactorMachineServiceStub machineServiceStub) {
        this.machineServiceStub = machineServiceStub;
    }

    public Mono<Machine> getMachine(Id request) {
        return machineServiceStub.getMachine(request);
    }

    public Mono<MachineQueryResult> getMachines(QueryRequest request) {
        return machineServiceStub.getMachines(request);
    }
}
