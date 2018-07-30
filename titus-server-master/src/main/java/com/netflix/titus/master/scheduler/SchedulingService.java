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

package com.netflix.titus.master.scheduler;

import java.util.Optional;

import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import rx.Observable;

/**
 */
public interface SchedulingService {

    String COMPONENT = "scheduler";

    /**
     * Adds a running task to the scheduler. This method is used to restore the scheduler state after system failover.
     */
    void addRunningTask(QueuableTask task, String hostname);

    /**
     * Adds a new, not scheduled yet, task to the scheduler.
     */
    void addTask(QueuableTask queuableTask);

    /**
     * Removes a task from the scheduler.
     * <p>
     * FIXME To remove a task, it should be enough to pass the task id. The other arguments represent an internal state of the scheduling component itself.
     */
    @Deprecated
    void removeTask(String taskid, QAttributes qAttributes, String hostname);

    /**
     * Returns the last known scheduling result for a task.
     *
     * @return {@link Optional#empty()} if the task is not found or the scheduling result otherwise
     */
    Optional<SchedulingResultEvent> findLastSchedulingResult(String taskId);

    /**
     * Observe Fenzo scheduling results for a task. The stream is completed when the task is successfully scheduled or
     * removed from the Fenzo queue.
     */
    Observable<SchedulingResultEvent> observeSchedulingResults(String taskId);
}
