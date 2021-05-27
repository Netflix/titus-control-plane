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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1EnvVar;

import static com.netflix.titus.master.kubernetes.pod.KubePodUtil.toV1EnvVar;

@Singleton
public class DefaultPodEnvFactory implements PodEnvFactory {

    @Inject
    public DefaultPodEnvFactory() {
    }

    @Override
    public Pair<Integer, List<V1EnvVar>> buildEnv(Job<?> job, Task task) {
        Map<String, String> envVars = new LinkedHashMap<>();
        envVars.put(KubeConstants.POD_ENV_TITUS_JOB_ID, task.getJobId());
        envVars.put(KubeConstants.POD_ENV_TITUS_TASK_ID, task.getId());
        envVars.put(KubeConstants.POD_ENV_NETFLIX_EXECUTOR, "titus");
        envVars.put(KubeConstants.POD_ENV_NETFLIX_INSTANCE_ID, task.getId());
        envVars.put(KubeConstants.POD_ENV_TITUS_TASK_INSTANCE_ID, task.getId());
        envVars.put(KubeConstants.POD_ENV_TITUS_TASK_ORIGINAL_ID, task.getOriginalId());
        if (task instanceof BatchJobTask) {
            BatchJobTask batchJobTask = (BatchJobTask) task;
            envVars.put(KubeConstants.POD_ENV_TITUS_TASK_INDEX, "" + batchJobTask.getIndex());
        }
        int systemEnvSize = envVars.size();

        Map<String, String> userEnv = job.getJobDescriptor().getContainer().getEnv();
        userEnv.forEach((k, v) -> {
            // do not let user env vars override system env vars
            if (!envVars.containsKey(k)) {
                envVars.put(k, v);
            }
        });

        return Pair.of(systemEnvSize, toV1EnvVar(envVars));
    }
}
