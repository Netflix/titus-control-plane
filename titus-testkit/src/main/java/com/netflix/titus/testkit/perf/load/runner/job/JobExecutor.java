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

package com.netflix.titus.testkit.perf.load.runner.job;

import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Task;
import reactor.core.publisher.Mono;
import rx.Completable;

public interface JobExecutor {

    String getName();

    String getJobId();

    boolean isSubmitted();

    List<Task> getActiveTasks();

    Mono<Void> awaitJobCompletion();

    Mono<Void> killJob();

    Mono<Void> killTask(String taskId);

    Mono<Void> evictTask(String taskId);

    Mono<Void> terminateAndShrink(String taskId);

    Mono<Void> updateInstanceCount(int min, int desired, int max);

    Mono<Void> scaleUp(int delta);

    Mono<Void> scaleDown(int delta);

    void shutdown();
}
