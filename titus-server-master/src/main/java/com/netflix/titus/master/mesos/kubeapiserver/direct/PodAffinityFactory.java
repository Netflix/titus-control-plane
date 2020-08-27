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

package com.netflix.titus.master.mesos.kubeapiserver.direct;

import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import io.kubernetes.client.openapi.models.V1Affinity;

/**
 * Builds pod affinity and ant-affinity rules for a job/task. This includes both the job level hard and soft
 * constraints, as well as Titus system level constraints.
 */
public interface PodAffinityFactory {

    /**
     * Returns Kubernetes {@link V1Affinity} rules for a task, and a map of key/value pairs that are added to
     * pod annotations.
     */
    Pair<V1Affinity, Map<String, String>> buildV1Affinity(Job<?> job, Task task);
}
