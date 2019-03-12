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

package com.netflix.titus.testkit.model.eviction;

import java.util.Optional;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.eviction.service.EvictionOperations;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class StubbedEvictionOperations implements EvictionOperations {

    private final StubbedEvictionData stubbedEvictionData;
    private final V3JobOperations jobOperations;

    StubbedEvictionOperations(StubbedEvictionData stubbedEvictionData, V3JobOperations jobOperations) {
        this.stubbedEvictionData = stubbedEvictionData;
        this.jobOperations = jobOperations;
    }

    @Override
    public Mono<Void> terminateTask(String taskId, String reason, String callerId) {
        return deferMono(() -> {
            Pair<Job<?>, Task> jobTaskPair = jobOperations.findTaskById(taskId).orElseThrow(() -> JobManagerException.taskNotFound(taskId));
            Job<?> job = jobTaskPair.getLeft();

            long quota = stubbedEvictionData.findEvictionQuota(Reference.job(job.getId())).map(EvictionQuota::getQuota).orElse(0L);
            if (quota <= 0) {
                throw EvictionException.noAvailableJobQuota(job, "No quota");
            }

            jobOperations.killTask(taskId, false, V3JobOperations.Trigger.Eviction, CallMetadata.newBuilder().withCallReason("Eviction").withCallerId("test").build()).block();
            stubbedEvictionData.setJobQuota(job.getId(), quota - 1);
        });
    }

    @Override
    public EvictionQuota getEvictionQuota(Reference reference) {
        return stubbedEvictionData.getEvictionQuota(reference);
    }

    @Override
    public Optional<EvictionQuota> findEvictionQuota(Reference reference) {
        return stubbedEvictionData.findEvictionQuota(reference);
    }

    @Override
    public Flux<EvictionEvent> events(boolean includeSnapshot) {
        return stubbedEvictionData.events(includeSnapshot);
    }

    private Mono<Void> deferMono(Runnable action) {
        return Mono.defer(() -> {
            action.run();
            return Mono.empty();
        });
    }
}
