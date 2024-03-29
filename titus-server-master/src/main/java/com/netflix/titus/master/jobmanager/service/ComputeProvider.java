/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import reactor.core.publisher.Mono;

public interface ComputeProvider {

    Mono<Void> launchTask(Job<?> job, Task task);

    /**
     * TODO {@link com.netflix.titus.master.kubernetes.client.DefaultDirectKubeApiServerIntegrator} contract is vague, and could be improved.
     */
    Mono<Void> terminateTask(Task task);

    default boolean isReadyForScheduling() {
        return true;
    }

    /**
     * Given a platform provider create error, resolve it to a reason code defined in {@link com.netflix.titus.api.jobmanager.model.job.TaskStatus}.
     */
    String resolveReasonCode(Throwable error);
}
