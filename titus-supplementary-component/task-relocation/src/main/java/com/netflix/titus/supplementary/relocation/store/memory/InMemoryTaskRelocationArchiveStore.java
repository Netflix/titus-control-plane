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

package com.netflix.titus.supplementary.relocation.store.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.inject.Singleton;

import com.netflix.titus.api.relocation.model.TaskRelocationStatus;
import com.netflix.titus.supplementary.relocation.store.TaskRelocationArchiveStore;
import reactor.core.publisher.Mono;

@Singleton
public class InMemoryTaskRelocationArchiveStore implements TaskRelocationArchiveStore {

    private final ConcurrentMap<String, List<TaskRelocationStatus>> taskRelocationStatusesByTaskId = new ConcurrentHashMap<>();

    @Override
    public Mono<Map<String, Optional<Throwable>>> createTaskRelocationStatuses(List<TaskRelocationStatus> taskRelocationStatuses) {
        return Mono.defer(() -> {
            Map<String, Optional<Throwable>> result = new HashMap<>();
            synchronized (taskRelocationStatusesByTaskId) {
                taskRelocationStatuses.forEach(status -> {
                            taskRelocationStatusesByTaskId.computeIfAbsent(
                                    status.getTaskId(),
                                    tid -> new ArrayList<>()
                            ).add(status);
                            result.put(status.getTaskId(), Optional.empty());
                        }
                );
            }
            return Mono.just(result);
        });
    }

    @Override
    public Mono<List<TaskRelocationStatus>> getTaskRelocationStatusList(String taskId) {
        return Mono.defer(() -> {
            List<TaskRelocationStatus> status = taskRelocationStatusesByTaskId.get(taskId);
            if(status == null) {
                return Mono.just(Collections.emptyList());
            }
            return Mono.just(status);
        });
    }
}
