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

package com.netflix.titus.master.kubernetes.pod.env;

import java.util.List;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import io.kubernetes.client.openapi.models.V1EnvVar;

public interface PodEnvFactory {
    /**
     * Builds pod env and returns a pair where the first value is an {@link Integer} index where the first user provided
     * env var begins and the second value is a list of {@link V1EnvVar} where the first part is system env vars while
     * the second part is user env vars.
     */
    Pair<Integer, List<V1EnvVar>> buildEnv(Job<?> job, Task task);
}
