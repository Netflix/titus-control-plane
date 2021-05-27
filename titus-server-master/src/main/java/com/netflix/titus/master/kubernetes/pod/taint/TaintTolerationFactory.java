/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes.pod.taint;

import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import io.kubernetes.client.openapi.models.V1Toleration;

/**
 * {@link TaintTolerationFactory} resolves taint tolerations for a task that should be added when a pod is created.
 */
public interface TaintTolerationFactory {

    List<V1Toleration> buildV1Toleration(Job<?> job, Task task, boolean useKubeScheduler);
}
