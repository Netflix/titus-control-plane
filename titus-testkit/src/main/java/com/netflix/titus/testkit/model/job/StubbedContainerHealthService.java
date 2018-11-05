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

package com.netflix.titus.testkit.model.job;

import java.util.Optional;

import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import reactor.core.publisher.Flux;

class StubbedContainerHealthService implements ContainerHealthService {

    private final StubbedJobData stubbedJobData;

    StubbedContainerHealthService(StubbedJobData stubbedJobData) {
        this.stubbedJobData = stubbedJobData;
    }

    @Override
    public String getName() {
        return "test";
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return stubbedJobData.findTask(taskId).map(task -> stubbedJobData.getTaskHealthStatus(taskId)).orElse(Optional.empty());
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        return null;
    }
}
