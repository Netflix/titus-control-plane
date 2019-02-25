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

package com.netflix.titus.runtime.connector.relocation;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.TaskRelocationQuery;
import com.netflix.titus.runtime.relocation.endpoint.RelocationGrpcModelConverters;
import reactor.core.publisher.Mono;

import static com.netflix.titus.runtime.relocation.endpoint.RelocationGrpcModelConverters.toCoreTaskRelocationPlan;

/**
 * {@link RelocationServiceClient} implementation that translates all invocation into underlying GRPC calls.
 */
@Singleton
public class RemoteRelocationServiceClient implements RelocationServiceClient {

    private static final Page ONE_ITEM_PAGE = Page.newBuilder().setPageSize(1).build();

    private final ReactorRelocationServiceStub transportRelocationClient;

    @Inject
    public RemoteRelocationServiceClient(ReactorRelocationServiceStub transportRelocationClient) {
        this.transportRelocationClient = transportRelocationClient;
    }

    @Override
    public Mono<Optional<TaskRelocationPlan>> findTaskRelocationPlan(String taskId) {
        return transportRelocationClient.getCurrentTaskRelocationPlans(TaskRelocationQuery.newBuilder()
                .setPage(ONE_ITEM_PAGE)
                .putFilteringCriteria("taskIds", taskId)
                .build()
        ).flatMap(plans -> {
            if (plans.getPlansList().isEmpty()) {
                return Mono.just(Optional.empty());
            }
            // Sanity check
            if (plans.getPlansList().size() > 1) {
                return Mono.error(new IllegalStateException("Received multiple relocation plans for the same task id: " + taskId));
            }
            return Mono.just(Optional.of(toCoreTaskRelocationPlan(plans.getPlansList().get(0))));
        });
    }

    @Override
    public Mono<List<TaskRelocationPlan>> findTaskRelocationPlans(Set<String> taskIds) {
        return transportRelocationClient.getCurrentTaskRelocationPlans(TaskRelocationQuery.newBuilder()
                .setPage(ONE_ITEM_PAGE)
                .putFilteringCriteria("taskIds", StringExt.concatenate(taskIds, ","))
                .build()
        ).flatMap(plans -> {
            List<TaskRelocationPlan> coreList = plans.getPlansList().stream()
                    .map(RelocationGrpcModelConverters::toCoreTaskRelocationPlan)
                    .collect(Collectors.toList());
            return Mono.just(coreList);
        });
    }
}
