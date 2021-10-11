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

package com.netflix.titus.gateway.service.v3;

import com.netflix.titus.grpc.protogen.SchedulingResultEvent;
import rx.Observable;

/**
 * Gateway service for the scheduler component.
 */
public interface SchedulerService {

    /**
     * Returns the last known scheduling result for a task.
     *
     * @return {@link Observable#empty()} if the task is not found or the scheduling result otherwise
     */
    Observable<SchedulingResultEvent> findLastSchedulingResult(String taskId);

    /**
     * Observe Fenzo scheduling results for a task. The stream is completed when the task is successfully scheduled or
     * removed from the Fenzo queue.
     */
    Observable<SchedulingResultEvent> observeSchedulingResults(String taskId);
}
