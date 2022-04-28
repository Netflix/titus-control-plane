/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.master.service.management.kube;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.service.management.kube.crd.F8IOCapacityGroup;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.Fabric8IOInformerMetrics;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;

class CapacityGroupInformerWrapper {

    private final SharedInformerFactory sharedInformerFactory;

    private final SharedIndexInformer<F8IOCapacityGroup> informer;
    private final Fabric8IOInformerMetrics<F8IOCapacityGroup> metrics;

    CapacityGroupInformerWrapper(NamespacedKubernetesClient kubeApiClient, TitusRuntime titusRuntime) {
        this.sharedInformerFactory = kubeApiClient.informers();
        this.informer = sharedInformerFactory.sharedIndexInformerFor(
                F8IOCapacityGroup.class,
                3000
        );
        this.sharedInformerFactory.startAllRegisteredInformers();
        this.metrics = new Fabric8IOInformerMetrics<>("capacityGroupInformer", informer, titusRuntime);
    }

    void shutdown() {
        metrics.close();
        sharedInformerFactory.stopAllRegisteredInformers();
    }

    SharedIndexInformer<F8IOCapacityGroup> getInformer() {
        return informer;
    }
}
