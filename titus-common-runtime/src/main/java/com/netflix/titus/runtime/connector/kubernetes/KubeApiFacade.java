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

package com.netflix.titus.runtime.connector.kubernetes;

import com.netflix.titus.runtime.connector.kubernetes.v1.V1OpportunisticResource;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CustomObjectsApi;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1Pod;

public interface KubeApiFacade {

    ApiClient getApiClient();

    CoreV1Api getCoreV1Api();

    CustomObjectsApi getCustomObjectsApi();

    SharedIndexInformer<V1Node> getNodeInformer();

    SharedIndexInformer<V1Pod> getPodInformer();

    SharedIndexInformer<V1PersistentVolume> getPersistentVolumeInformer();

    SharedIndexInformer<V1PersistentVolumeClaim> getPersistentVolumeClaimInformer();

    /**
     * Provide information about how up to date the pod informer data is. If the pod informer is connected, and synced
     * the staleness is 0.
     *
     * @return -1 if the pod informer was never synced or the data staleness time
     */
    default long getPodInformerStaleness() {
        return 0;
    }

    /**
     * Returns true, if the Kubernetes integration subsystem is ready for scheduling.
     */
    default boolean isReadyForScheduling() {
        return true;
    }

    SharedIndexInformer<V1OpportunisticResource> getOpportunisticResourceInformer();
}
