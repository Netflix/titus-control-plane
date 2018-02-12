/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.perf.load.job;

import java.util.List;

import io.netflix.titus.api.jobmanager.model.job.Task;
import rx.Completable;
import rx.Observable;

public interface JobExecutor {

    String getName();

    String getJobId();

    boolean isSubmitted();

    List<Task> getActiveTasks();

    Completable awaitJobCompletion();

    Observable<Void> killJob();

    Observable<Void> killTask(String taskId);

    Observable<Void> terminateAndShrink(String taskId);

    Observable<Void> updateInstanceCount(int min, int desired, int max);

    Observable<Void> scaleUp(int delta);

    Observable<Void> scaleDown(int delta);

    void shutdown();
}
