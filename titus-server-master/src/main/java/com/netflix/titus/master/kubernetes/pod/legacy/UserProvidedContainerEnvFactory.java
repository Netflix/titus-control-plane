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

import java.util.HashMap;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;

public class UserProvidedContainerEnvFactory implements ContainerEnvFactory {

    private static final ContainerEnvFactory INSTANCE = new UserProvidedContainerEnvFactory();

    @Override
    public Map<String, String> buildContainerEnv(Job<?> job, Task task) {
        Map<String, String> env = new HashMap<>();
        job.getJobDescriptor().getContainer().getEnv().forEach((k, v) -> {
            if (v != null) {
                env.put(k, v);
            }
        });
        return env;
    }

    public static ContainerEnvFactory getInstance() {
        return INSTANCE;
    }
}
