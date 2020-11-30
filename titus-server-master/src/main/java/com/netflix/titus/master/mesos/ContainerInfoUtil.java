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

package com.netflix.titus.master.mesos;

import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Container;
import com.netflix.titus.api.jobmanager.model.job.Task;
import io.titanframework.messages.TitanProtos;

public class ContainerInfoUtil {

    public static void setContainerInfoEnvVariables(TitanProtos.ContainerInfo.Builder containerInfoBuilder, Container container, Task task) {
        container.getEnv().forEach((k, v) -> {
            if (v != null) {
                containerInfoBuilder.putUserProvidedEnv(k, v);
            }
        });

        containerInfoBuilder.putTitusProvidedEnv("TITUS_JOB_ID", task.getJobId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("NETFLIX_EXECUTOR", "titus");
        containerInfoBuilder.putTitusProvidedEnv("NETFLIX_INSTANCE_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INSTANCE_ID", task.getId());
        containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_ORIGINAL_ID", task.getOriginalId());
        if (task instanceof BatchJobTask) {
            BatchJobTask batchJobTask = (BatchJobTask) task;
            containerInfoBuilder.putTitusProvidedEnv("TITUS_TASK_INDEX", "" + batchJobTask.getIndex());
        }
    }
}
