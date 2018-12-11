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

package com.netflix.titus.supplementary.relocation.store;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import reactor.core.publisher.Mono;

/**
 * Store interface for persisting large amount of data, which (most of the time) are never updated. This
 * interface may be backed by the same or different store as {@link TaskRelocationStore}.
 */
public interface TaskRelocationResultStore {

    /**
     * Creates or updates a record in the database.
     */
    Mono<Map<String, Optional<Throwable>>> createTaskRelocationStatuses(List<TaskRelocationStatus> taskRelocationStatuses);

    /**
     * Returns all archived task relocation statuses, or an empty list if non is found.
     */
    Mono<List<TaskRelocationStatus>> getTaskRelocationStatusList(String taskId);
}
