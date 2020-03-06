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

import com.netflix.titus.master.mesos.kubeapiserver.model.v1.V1OpportunisticResource;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;

public interface KubeApiFacade {

    ApiClient getApiClient();

    CoreV1Api getCoreV1Api();

    CustomObjectsApi getCustomObjectsApi();

    SharedIndexInformer<V1Node> getNodeInformer();

    SharedIndexInformer<V1Pod> getPodInformer();

    SharedIndexInformer<V1OpportunisticResource> getOpportunisticResourceInformer();
}
