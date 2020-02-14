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

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeList;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.util.CallGeneratorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.master.mesos.kubeapiserver.KubeUtil.createSharedInformerFactory;

@Singleton
public class DefaultKubeApiFactory implements KubeApiFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultKubeApiFactory.class);

    private static final String KUBERNETES_NAMESPACE = "default";

    private final DirectKubeConfiguration configuration;
    private final ApiClient apiClient;
    private final CoreV1Api coreV1Api;

    private final Object activationLock = new Object();

    private volatile SharedInformerFactory sharedInformerFactory;
    private volatile SharedIndexInformer<V1Node> nodeInformer;
    private volatile SharedIndexInformer<V1Pod> podInformer;

    private volatile boolean deactivated;

    @Inject
    public DefaultKubeApiFactory(DirectKubeConfiguration configuration, ApiClient apiClient) {
        this.configuration = configuration;
        this.apiClient = apiClient;
        this.coreV1Api = new CoreV1Api(apiClient);
    }

    @PreDestroy
    public void shutdown() {
        if (sharedInformerFactory != null) {
            sharedInformerFactory.stopAllRegisteredInformers();
        }
    }

    @Deactivator
    public void deactivate() {
        if (!deactivated) {
            synchronized (activationLock) {
                shutdown();
                this.deactivated = true;
            }
        }
    }

    @Override
    public ApiClient getApiClient() {
        activate();
        return apiClient;
    }

    @Override
    public CoreV1Api getCoreV1Api() {
        activate();
        return coreV1Api;
    }

    @Override
    public SharedIndexInformer<V1Node> getNodeInformer() {
        activate();
        return nodeInformer;
    }

    @Override
    public SharedIndexInformer<V1Pod> getPodInformer() {
        activate();
        return podInformer;
    }

    private void activate() {
        synchronized (activationLock) {
            if (deactivated) {
                throw new IllegalStateException("Deactivated");
            }

            if (sharedInformerFactory != null) {
                return;
            }

            try {
                this.sharedInformerFactory = createSharedInformerFactory(
                        "kube-api-server-integrator-shared-informer-",
                        apiClient
                );

                this.nodeInformer = createNodeInformer(sharedInformerFactory, coreV1Api);
                this.podInformer = createPodInformer(sharedInformerFactory, coreV1Api);

                sharedInformerFactory.startAllRegisteredInformers();

                logger.info("Kube node and pod informers activated");
            } catch (Exception e) {
                logger.error("Could not initialize Kube client shared informer", e);
                if (sharedInformerFactory != null) {
                    ExceptionExt.silent(() -> sharedInformerFactory.stopAllRegisteredInformers());
                }
                sharedInformerFactory = null;
                nodeInformer = null;
                podInformer = null;
                throw e;
            }
        }
    }

    private SharedIndexInformer<V1Node> createNodeInformer(SharedInformerFactory sharedInformerFactory, CoreV1Api api) {
        return sharedInformerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> api.listNodeCall(
                        null,
                        null,
                        null,
                        null,
                        null,
                        params.resourceVersion,
                        params.timeoutSeconds,
                        params.watch,
                        null,
                        null),
                V1Node.class,
                V1NodeList.class,
                configuration.getKubeApiServerIntegratorRefreshIntervalMs()
        );
    }

    private SharedIndexInformer<V1Pod> createPodInformer(SharedInformerFactory sharedInformerFactory, CoreV1Api api) {
        return sharedInformerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> api.listNamespacedPodCall(
                        KUBERNETES_NAMESPACE,
                        null,
                        null,
                        null,
                        null,
                        null,
                        params.resourceVersion,
                        params.timeoutSeconds,
                        params.watch,
                        null,
                        null
                ),
                V1Pod.class,
                V1PodList.class,
                configuration.getKubeApiServerIntegratorRefreshIntervalMs()
        );
    }
}
