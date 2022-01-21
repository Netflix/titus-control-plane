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

package com.netflix.titus.master.kubernetes.pod.legacy;

import java.util.List;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import io.kubernetes.client.openapi.models.V1EnvVar;

/**
 * Build container environment that is added to pod env section.
 */
public interface ContainerEnvFactory {

    /**
     * A pair of two things:
     * Right -> Map of env vars
     * Left -> A list of strings representing which of those env vars were injected by Titus and not provided by the user
     *
     * @return none null environment variables map
     */
    Pair<List<String>, Map<String, String>> buildContainerEnv(Job<?> job, Task task);
}
