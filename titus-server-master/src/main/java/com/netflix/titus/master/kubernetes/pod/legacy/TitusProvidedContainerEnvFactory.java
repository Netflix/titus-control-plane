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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.kubernetes.KubeConstants;

public class TitusProvidedContainerEnvFactory implements ContainerEnvFactory {

    private static final ContainerEnvFactory INSTANCE = new TitusProvidedContainerEnvFactory();

    private static final List<String> commonPodEnvVars = new ArrayList<>(Arrays.asList(
            KubeConstants.POD_ENV_TITUS_JOB_ID,
            KubeConstants.POD_ENV_TITUS_TASK_ID,
            KubeConstants.POD_ENV_NETFLIX_EXECUTOR,
            KubeConstants.POD_ENV_NETFLIX_INSTANCE_ID,
            KubeConstants.POD_ENV_TITUS_TASK_INSTANCE_ID,
            KubeConstants.POD_ENV_TITUS_TASK_ORIGINAL_ID
    ));

    private static final List<String> batchPodEnvVars = Collections.singletonList(
            KubeConstants.POD_ENV_TITUS_TASK_INDEX
    );

    @Override
    public Pair<List<String>, Map<String, String>> buildContainerEnv(Job<?> job, Task task) {
        Map<String, String> env = new HashMap<>();
        env.put(KubeConstants.POD_ENV_TITUS_JOB_ID, task.getJobId());
        env.put(KubeConstants.POD_ENV_TITUS_TASK_ID, task.getId());
        env.put(KubeConstants.POD_ENV_NETFLIX_EXECUTOR, "titus");
        env.put(KubeConstants.POD_ENV_NETFLIX_INSTANCE_ID, task.getId());
        env.put(KubeConstants.POD_ENV_TITUS_TASK_INSTANCE_ID, task.getId());
        env.put(KubeConstants.POD_ENV_TITUS_TASK_ORIGINAL_ID, task.getOriginalId());
        // In TitusProvidedContainerEnvFactory, systemEnvNames ends up having everything we add
        List<String> systemEnvNames = new ArrayList<>(commonPodEnvVars);
        if (task instanceof BatchJobTask) {
            BatchJobTask batchJobTask = (BatchJobTask) task;
            env.put(KubeConstants.POD_ENV_TITUS_TASK_INDEX, "" + batchJobTask.getIndex());
            systemEnvNames.addAll(batchPodEnvVars);
        }
        return Pair.of(systemEnvNames, env);
    }

    public static ContainerEnvFactory getInstance() {
        return INSTANCE;
    }
}
