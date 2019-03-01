/*
 *
 *  * Copyright 2019 Netflix, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.netflix.titus.api.jobactivity.store;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * This interface represents the publisher/producer side of a job activity store that
 * records job activity records to be consumed by a separate consumer.
 */
public interface JobActivityPublisherStore {

    // Publishes a single job record
    // TODO(Andrew L): Add CallMetadata when ready
     Mono<Void> publishJob(Job<?> job);

     // Publishes a single task record
     // TODO(Andrew L): Add CallMetadata when ready
     Mono<Void> publishTask(Task task);

     // Returns all current store records
    Flux<JobActivityPublisherRecord> getRecords();

    // Returns the current number of store records
    // This exists as a faster alternative to getRecords()
    Mono<Integer> getSize();
}