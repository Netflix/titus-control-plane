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

package com.netflix.titus.ext.jooq.relocation;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan.TaskRelocationReason;
import com.netflix.titus.ext.jooq.relocation.schema.JRelocation;
import com.netflix.titus.ext.jooq.relocation.schema.tables.records.JRelocationPlanRecord;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationStore;
import org.jooq.DSLContext;
import org.jooq.Delete;
import org.jooq.Result;
import org.jooq.StoreQuery;
import org.jooq.impl.DSL;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import static com.netflix.titus.ext.jooq.relocation.schema.tables.JRelocationPlan.RELOCATION_PLAN;

@Singleton
public class JooqTaskRelocationStore implements TaskRelocationStore {

    private final DSLContext dslContext;

    private final ConcurrentMap<String, TaskRelocationPlan> plansByTaskId = new ConcurrentHashMap<>();

    @Inject
    public JooqTaskRelocationStore(DSLContext dslContext) {
        this.dslContext = dslContext;
        createSchemaIfNotExist();
        load();
    }

    private void createSchemaIfNotExist() {
        dslContext.createSchemaIfNotExists(JRelocation.RELOCATION).execute();
        dslContext.createTableIfNotExists(RELOCATION_PLAN)
                .column(RELOCATION_PLAN.TASK_ID)
                .column(RELOCATION_PLAN.REASON_CODE)
                .column(RELOCATION_PLAN.REASON_MESSAGE)
                .column(RELOCATION_PLAN.DECISION_TIME)
                .column(RELOCATION_PLAN.RELOCATION_TIME)
                .constraint(DSL.constraint("pk_relocation_plan_task_id").primaryKey(RELOCATION_PLAN.TASK_ID))
                .execute();
    }

    private void load() {
        Result<JRelocationPlanRecord> allRows = dslContext.selectFrom(RELOCATION_PLAN).fetch();
        for (JRelocationPlanRecord record : allRows) {
            plansByTaskId.put(
                    record.getTaskId(),
                    TaskRelocationPlan.newBuilder()
                            .withTaskId(record.getTaskId())
                            .withReason(TaskRelocationReason.valueOf(record.getReasonCode()))
                            .withReasonMessage(record.getReasonMessage())
                            .withRelocationTime(record.getRelocationTime().getTime())
                            .build()
            );
        }
    }

    @Override
    public Mono<Map<String, Optional<Throwable>>> createOrUpdateTaskRelocationPlans(List<TaskRelocationPlan> taskRelocationPlans) {
        if (taskRelocationPlans.isEmpty()) {
            return Mono.empty();
        }

        return Mono.defer(() -> {
            List<StoreQuery<JRelocationPlanRecord>> queries = taskRelocationPlans.stream().map(this::newCreateOrUpdateQuery).collect(Collectors.toList());
            CompletionStage<Void> asyncAction = DSL.using(dslContext.configuration())
                    .transactionAsync(configuration -> configuration.dsl()
                            .batch(queries)
                            .execute()
                    );

            MonoProcessor<Map<String, Optional<Throwable>>> callerProcessor = MonoProcessor.create();
            asyncAction.handle((result, error) -> {
                Map<String, Optional<Throwable>> resultMap = new HashMap<>();
                if (error == null) {
                    taskRelocationPlans.forEach(p -> {
                        resultMap.put(p.getTaskId(), Optional.empty());
                        plansByTaskId.put(p.getTaskId(), p);
                    });

                    callerProcessor.onNext(resultMap);
                } else {
                    callerProcessor.onError(error);
                }
                return null;
            });

            return callerProcessor;
        });
    }

    @Override
    public Mono<Map<String, TaskRelocationPlan>> getAllTaskRelocationPlans() {
        return Mono.just(Collections.unmodifiableMap(plansByTaskId));
    }

    @Override
    public Mono<Map<String, Optional<Throwable>>> removeTaskRelocationPlans(Set<String> toRemove) {
        if (toRemove.isEmpty()) {
            return Mono.empty();
        }

        return Mono.defer(() -> {
            List<Delete<JRelocationPlanRecord>> deletes = toRemove.stream()
                    .filter(plansByTaskId::containsKey)
                    .map(this::newDelete)
                    .collect(Collectors.toList());

            CompletionStage<Void> asyncAction = DSL.using(dslContext.configuration())
                    .transactionAsync(configuration -> configuration.dsl()
                            .batch(deletes)
                            .execute()
                    );

            MonoProcessor<Map<String, Optional<Throwable>>> callerProcessor = MonoProcessor.create();
            asyncAction.handle((result, error) -> {
                if (error == null) {
                    toRemove.forEach(plansByTaskId::remove);
                    Map<String, Optional<Throwable>> resultMap = toRemove.stream()
                            .collect(Collectors.toMap(taskId -> taskId, taskId -> Optional.empty()));
                    callerProcessor.onNext(resultMap);
                } else {
                    callerProcessor.onError(error);
                }
                return null;
            });

            return callerProcessor;
        });
    }

    private StoreQuery<JRelocationPlanRecord> newCreateOrUpdateQuery(TaskRelocationPlan relocationPlan) {
        StoreQuery<JRelocationPlanRecord> storeQuery;

        if (plansByTaskId.containsKey(relocationPlan.getTaskId())) {
            storeQuery = dslContext.updateQuery(RELOCATION_PLAN);
        } else {
            storeQuery = dslContext.insertQuery(RELOCATION_PLAN);
            storeQuery.addValue(RELOCATION_PLAN.TASK_ID, relocationPlan.getTaskId());
        }

        storeQuery.addValue(RELOCATION_PLAN.REASON_CODE, relocationPlan.getReason().name());
        storeQuery.addValue(RELOCATION_PLAN.REASON_MESSAGE, relocationPlan.getReasonMessage());
        storeQuery.addValue(RELOCATION_PLAN.DECISION_TIME, new Timestamp(relocationPlan.getDecisionTime()));
        storeQuery.addValue(RELOCATION_PLAN.RELOCATION_TIME, new Timestamp(relocationPlan.getRelocationTime()));

        return storeQuery;
    }

    private Delete<JRelocationPlanRecord> newDelete(String taskId) {
        return dslContext.delete(RELOCATION_PLAN).where(RELOCATION_PLAN.TASK_ID.eq(taskId));
    }
}
