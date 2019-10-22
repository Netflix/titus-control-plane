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

package com.netflix.titus.master.machine.endpoint.grpc;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.Pagination;
import com.netflix.titus.grpc.protogen.v4.Id;
import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineQueryResult;
import com.netflix.titus.grpc.protogen.v4.QueryRequest;
import reactor.core.publisher.Mono;

@Singleton
public class ReactorMachineGrpcService {

    private static final ScheduleDescriptor SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(ReactorMachineGrpcService.class.getSimpleName())
            .withDescription("Machine resources evaluator")
            .withInterval(Duration.ofSeconds(1))
            .withRetryerSupplier(Retryers::never)
            .withTimeout(Duration.ofSeconds(5))
            .build();

    private final MachineResourcesEvaluator evaluator;
    private final TitusRuntime titusRuntime;

    private volatile Map<String, Machine> machinesById = Collections.emptyMap();

    private ScheduleReference scheduleRef;

    @Inject
    public ReactorMachineGrpcService(MachineResourcesEvaluator evaluator, TitusRuntime titusRuntime) {
        this.evaluator = evaluator;
        this.titusRuntime = titusRuntime;
    }

    @Activator
    public void enterActiveMode() {
        this.machinesById = evaluator.evaluate();
        this.scheduleRef = titusRuntime.getLocalScheduler().schedule(
                SCHEDULE_DESCRIPTOR,
                context -> machinesById = evaluator.evaluate(),
                true
        );
    }

    @PreDestroy
    public void shutdown() {
        Evaluators.acceptNotNull(scheduleRef, ScheduleReference::cancel);
    }

    public Mono<Machine> getMachine(Id request) {
        return Mono.fromCallable(() -> {
            Machine machine = machinesById.get(request.getId());
            if (machine == null) {
                throw new IllegalArgumentException(String.format("Machine with id %s not found", request.getId()));
            }
            return machine;
        });
    }

    public Mono<MachineQueryResult> getMachines(QueryRequest request) {
        return Mono.fromCallable(() -> {
            // Copy first as `machinesById` is volatile and may change at any time.
            Map<String, Machine> machines = machinesById;
            return MachineQueryResult.newBuilder()
                    .addAllItems(machines.values())
                    .setPagination(Pagination.newBuilder()
                            .setCurrentPage(Page.newBuilder()
                                    .setPageSize(machines.size())
                                    .build()
                            )
                            .setTotalItems(machines.size())
                            .setHasMore(false)
                            .build()
                    )
                    .build();
        });
    }
}