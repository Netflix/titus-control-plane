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

package com.netflix.titus.master.kubernetes.client;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.kubernetes.DefaultKubeApiFacade;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.ApiClient;

@Singleton
public class JobControllerKubeApiFacade extends DefaultKubeApiFacade {

    private final TitusRuntime titusRuntime;

    @Inject
    public JobControllerKubeApiFacade(DirectKubeConfiguration configuration, ApiClient apiClient, TitusRuntime titusRuntime) {
        super(configuration, apiClient, titusRuntime);
        this.titusRuntime = titusRuntime;
    }

    @Override
    protected <T extends KubernetesObject> SharedIndexInformer<T> customizeInformer(String name, SharedIndexInformer<T> informer) {
        return titusRuntime.getFitFramework().isActive() ? new FitSharedIndexInformer<>(name, informer, titusRuntime) : informer;
    }
}
