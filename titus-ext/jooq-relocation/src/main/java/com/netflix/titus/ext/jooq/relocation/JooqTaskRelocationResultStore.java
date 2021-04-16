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
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.cache.Cache;
import com.netflix.titus.common.util.cache.Caches;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.ext.jooq.JooqUtils;
import com.netflix.titus.ext.jooq.relocation.tables.records.RelocationStatusRecord;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationResultStore;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.jooq.StoreQuery;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

@Singleton
public class JooqTaskRelocationResultStore implements TaskRelocationResultStore {

    private static final int MAX_TEXT_LENGTH = 2048;

    private final DSLContext dslContext;

    private final Cache<String, TaskRelocationStatus> statusesByTaskId;

    @Inject
    public JooqTaskRelocationResultStore(DSLContext dslContext, TitusRuntime titusRuntime) {
        this.dslContext = dslContext;
        this.statusesByTaskId = Caches.instrumentedCacheWithMaxSize(
                100_000,
                "titus.ext.jooq.relocationResultStore",
                titusRuntime.getRegistry()
        );
    }

    @VisibleForTesting
    Mono<Void> clearStore() {
        return JooqUtils.executeAsyncMono(() -> dslContext.truncateTable(Relocation.RELOCATION.RELOCATION_STATUS).execute(), dslContext).then();
    }

    @Override
    public Mono<Map<String, Optional<Throwable>>> createTaskRelocationStatuses(List<TaskRelocationStatus> taskRelocationStatuses) {
        return Mono.defer(() -> {
            CompletionStage<int[]> asyncAction = JooqUtils.executeAsync(() -> {
                loadToCache(findNotCached(taskRelocationStatuses), dslContext.configuration());

                List<StoreQuery<RelocationStatusRecord>> queries = taskRelocationStatuses.stream()
                        .map(this::newCreateOrUpdateQuery)
                        .collect(Collectors.toList());

                return dslContext
                        .batch(queries)
                        .execute();
            }, dslContext);

            MonoProcessor<Map<String, Optional<Throwable>>> callerProcessor = MonoProcessor.create();
            asyncAction.handle((result, error) -> {
                Map<String, Optional<Throwable>> resultMap = new HashMap<>();
                if (error == null) {
                    taskRelocationStatuses.forEach(p -> {
                        resultMap.put(p.getTaskId(), Optional.empty());
                        statusesByTaskId.put(p.getTaskId(), p);
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
    public Mono<List<TaskRelocationStatus>> getTaskRelocationStatusList(String taskId) {
        return Mono.defer(() -> {
            TaskRelocationStatus status = statusesByTaskId.getIfPresent(taskId);
            if (status != null) {
                return Mono.just(Collections.singletonList(status));
            }

            CompletionStage<Void> asyncAction = JooqUtils.executeAsync(() -> {
                loadToCache(Collections.singleton(taskId), dslContext.configuration());
                return null;
            }, dslContext);

            MonoProcessor<List<TaskRelocationStatus>> callerProcessor = MonoProcessor.create();
            asyncAction.handle((result, error) -> {
                if (error == null) {
                    TaskRelocationStatus loadedStatus = statusesByTaskId.getIfPresent(taskId);
                    callerProcessor.onNext(loadedStatus == null ? Collections.emptyList() : Collections.singletonList(loadedStatus));
                } else {
                    callerProcessor.onError(error);
                }
                return null;
            });

            return callerProcessor;
        });
    }

    /**
     * Remove from cache garbage collected entries.
     */
    void removeFromCache(List<Pair<String, Long>> toRemove) {
        toRemove.forEach(p -> {
            String taskId = p.getLeft();
            long timestamp = p.getRight();
            TaskRelocationStatus status = statusesByTaskId.getIfPresent(taskId);
            if (status != null && status.getTimestamp() == timestamp) {
                statusesByTaskId.invalidate(taskId);
            }
        });
    }

    private Set<String> findNotCached(List<TaskRelocationStatus> taskRelocationStatuses) {
        return taskRelocationStatuses.stream()
                .map(TaskRelocationStatus::getTaskId)
                .filter(taskId -> statusesByTaskId.getIfPresent(taskId) == null)
                .collect(Collectors.toSet());
    }

    private void loadToCache(Set<String> notCached, Configuration configuration) {
        Result<RelocationStatusRecord> loaded = configuration.dsl()
                .selectFrom(Relocation.RELOCATION.RELOCATION_STATUS)
                .where(Relocation.RELOCATION.RELOCATION_STATUS.TASK_ID.in(notCached)).fetch();
        loaded.forEach(record ->
                statusesByTaskId.put(record.getTaskId(),
                        TaskRelocationStatus.newBuilder()
                                .withTaskId(record.getTaskId())
                                .withState(TaskRelocationStatus.TaskRelocationState.valueOf(record.getRelocationState()))
                                .withStatusCode(record.getStatusCode())
                                .withStatusMessage(record.getStatusMessage())
                                .withTimestamp(record.getRelocationExecutionTime().getTime())
                                .withTaskRelocationPlan(TaskRelocationPlan.newBuilder()
                                        .withTaskId(record.getTaskId())
                                        .withReason(TaskRelocationPlan.TaskRelocationReason.valueOf(record.getReasonCode()))
                                        .withReasonMessage(record.getReasonMessage())
                                        .withDecisionTime(record.getRelocationDecisionTime().getTime())
                                        .withRelocationTime(record.getRelocationPlanTime().getTime())
                                        .build()
                                )
                                .withTimestamp(record.getRelocationExecutionTime().getTime())
                                .build()
                ));
    }

    private StoreQuery<RelocationStatusRecord> newCreateOrUpdateQuery(TaskRelocationStatus relocationStatus) {
        StoreQuery<RelocationStatusRecord> storeQuery;

        if (statusesByTaskId.getIfPresent(relocationStatus.getTaskId()) != null) {
            storeQuery = dslContext.updateQuery(Relocation.RELOCATION.RELOCATION_STATUS);
        } else {
            storeQuery = dslContext.insertQuery(Relocation.RELOCATION.RELOCATION_STATUS);
            storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.TASK_ID, relocationStatus.getTaskId());
        }

        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_STATE, relocationStatus.getState().name());
        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.STATUS_CODE, relocationStatus.getStatusCode());
        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.STATUS_MESSAGE, toLengthLimitedVarchar(relocationStatus.getStatusMessage()));
        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.REASON_CODE, relocationStatus.getTaskRelocationPlan().getReason().name());
        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.REASON_MESSAGE, toLengthLimitedVarchar(relocationStatus.getTaskRelocationPlan().getReasonMessage()));
        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_DECISION_TIME, new Timestamp(relocationStatus.getTaskRelocationPlan().getDecisionTime()));
        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_PLAN_TIME, new Timestamp(relocationStatus.getTaskRelocationPlan().getRelocationTime()));
        storeQuery.addValue(Relocation.RELOCATION.RELOCATION_STATUS.RELOCATION_EXECUTION_TIME, new Timestamp(relocationStatus.getTimestamp()));

        return storeQuery;
    }

    private String toLengthLimitedVarchar(String text) {
        return text.length() <= MAX_TEXT_LENGTH ? text : text.substring(0, MAX_TEXT_LENGTH);
    }
}
