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

package com.netflix.titus.master.kubernetes.pod;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.master.kubernetes.pod.v0.V0SpecPodFactory;
import com.netflix.titus.master.kubernetes.pod.v1.V1SpecPodFactory;
import io.kubernetes.client.openapi.models.V1Pod;

@Singleton
public class RouterPodFactory implements PodFactory {

    private final KubePodConfiguration configuration;
    private final V0SpecPodFactory v0SpecPodFactory;
    private final V1SpecPodFactory v1SpecPodFactory;

    @Inject
    public RouterPodFactory(KubePodConfiguration configuration,
                            V0SpecPodFactory v0SpecPodFactory,
                            V1SpecPodFactory v1SpecPodFactory) {
        this.configuration = configuration;
        this.v0SpecPodFactory = v0SpecPodFactory;
        this.v1SpecPodFactory = v1SpecPodFactory;
    }

    @Override
    public V1Pod buildV1Pod(Job<?> job, Task task, boolean useKubeScheduler, boolean useKubePv) {
        String podSpecVersion = configuration.getPodSpecVersion();
        if (podSpecVersion.equals("v1")) {
            return v1SpecPodFactory.buildV1Pod(job, task, useKubeScheduler, useKubePv);
        }
        return v0SpecPodFactory.buildV1Pod(job, task, useKubeScheduler, useKubePv);
    }
}
