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

package com.netflix.titus.runtime.connector.kubernetes.std;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiException;
import com.netflix.titus.runtime.connector.kubernetes.KubeConnectorConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.KubeUtil;
import io.kubernetes.client.common.KubernetesObject;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimList;
import io.kubernetes.client.openapi.models.V1PersistentVolumeList;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.CallGeneratorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import static com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiClients.createSharedInformerFactory;

@Singleton
public class DefaultStdStdKubeApiFacade implements StdKubeApiFacade {

    private static final Logger logger = LoggerFactory.getLogger(DefaultStdStdKubeApiFacade.class);

    public static final String FAILED = "Failed";
    public static final String BACKGROUND = "Background";

    private static final String KUBERNETES_NAMESPACE = "default";

    private final KubeConnectorConfiguration configuration;

    private final ApiClient apiClient;
    private final CoreV1Api coreV1Api;
    private final TitusRuntime titusRuntime;

    private final Object activationLock = new Object();

    private volatile SharedInformerFactory sharedInformerFactory;
    private volatile SharedIndexInformer<V1Node> nodeInformer;
    private volatile SharedIndexInformer<V1Pod> podInformer;
    private volatile SharedIndexInformer<V1PersistentVolume> persistentVolumeInformer;
    private volatile SharedIndexInformer<V1PersistentVolumeClaim> persistentVolumeClaimInformer;

    private StdKubeInformerMetrics<V1Node> nodeInformerMetrics;
    private StdKubeInformerMetrics<V1Pod> podInformerMetrics;
    private StdKubeInformerMetrics<V1PersistentVolume> persistentVolumeInformerMetrics;
    private StdKubeInformerMetrics<V1PersistentVolumeClaim> persistentVolumeClaimInformerMetrics;

    private volatile boolean deactivated;

    @Inject
    public DefaultStdStdKubeApiFacade(KubeConnectorConfiguration configuration, ApiClient apiClient, TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.apiClient = apiClient;
        this.coreV1Api = new CoreV1Api(apiClient);
        this.titusRuntime = titusRuntime;
    }

    @PreDestroy
    public void shutdown() {
        if (sharedInformerFactory != null) {
            sharedInformerFactory.stopAllRegisteredInformers();
        }
        Evaluators.acceptNotNull(nodeInformerMetrics, StdKubeInformerMetrics::shutdown);
        Evaluators.acceptNotNull(podInformerMetrics, StdKubeInformerMetrics::shutdown);
        Evaluators.acceptNotNull(persistentVolumeInformerMetrics, StdKubeInformerMetrics::shutdown);
        Evaluators.acceptNotNull(persistentVolumeClaimInformerMetrics, StdKubeInformerMetrics::shutdown);
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
    public void deleteNode(String nodeName) throws KubeApiException {
        try {
            coreV1Api.deleteNode(
                    nodeName,
                    null,
                    null,
                    0,
                    null,
                    BACKGROUND,
                    null
            );
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public void createNamespacedPod(String namespace, V1Pod pod) throws KubeApiException {
        try {
            coreV1Api.createNamespacedPod(namespace, pod, null, null, null);
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public Mono<V1Pod> createNamespacedPodAsync(String namespace, V1Pod pod) {
        return KubeUtil.toReact(handler ->
                coreV1Api.createNamespacedPodAsync(namespace, pod, null, null, null, handler)
        );
    }

    @Override
    public void deleteNamespacedPod(String namespace, String podName) throws KubeApiException {
        try {
            coreV1Api.deleteNamespacedPod(
                    podName,
                    namespace,
                    null,
                    null,
                    0,
                    null,
                    BACKGROUND,
                    null
            );
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public void deleteNamespacedPod(String namespace, String podName, int deleteGracePeriod) throws KubeApiException {
        try {
            coreV1Api.deleteNamespacedPod(
                    podName,
                    namespace,
                    null,
                    null,
                    deleteGracePeriod,
                    null,
                    null,
                    null
            );
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public void deleteNamespacedPersistentVolumeClaim(String namespace, String volumeClaimName) throws KubeApiException {
        try {
            coreV1Api.deleteNamespacedPersistentVolumeClaim(
                    volumeClaimName,
                    namespace,
                    null,
                    null,
                    0,
                    null,
                    null,
                    null
            );
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public void replacePersistentVolume(V1PersistentVolume persistentVolume) throws KubeApiException {
        try {
            coreV1Api.replacePersistentVolume(
                    KubeUtil.getMetadataName(persistentVolume.getMetadata()),
                    persistentVolume,
                    null,
                    null,
                    null
            );
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public void deletePersistentVolume(String volumeName) throws KubeApiException {
        try {
            coreV1Api.deletePersistentVolume(
                    volumeName,
                    null,
                    null,
                    0,
                    null,
                    null,
                    null
            );
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
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

    @Override
    public SharedIndexInformer<V1PersistentVolume> getPersistentVolumeInformer() {
        activate();
        return persistentVolumeInformer;
    }

    @Override
    public SharedIndexInformer<V1PersistentVolumeClaim> getPersistentVolumeClaimInformer() {
        activate();
        return persistentVolumeClaimInformer;
    }

    @Override
    public void createPersistentVolume(V1PersistentVolume v1PersistentVolume) throws KubeApiException {
        try {
            coreV1Api.createPersistentVolume(v1PersistentVolume, null, null, null);
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public void createNamespacedPersistentVolumeClaim(String namespace, V1PersistentVolumeClaim v1PersistentVolumeClaim) throws KubeApiException {
        try {
            coreV1Api.createNamespacedPersistentVolumeClaim(namespace, v1PersistentVolumeClaim, null, null, null);
        } catch (ApiException e) {
            throw new KubeApiException(e);
        }
    }

    @Override
    public long getPodInformerStaleness() {
        // TODO synced is set to true on first successful execution. We need to change this logic, once we have better insight into the informer loop.
        return podInformer != null && podInformer.hasSynced() ? 0 : -1;
    }

    @Override
    public boolean isReadyForScheduling() {
        return getPodInformerStaleness() == 0;
    }

    protected <T extends KubernetesObject> SharedIndexInformer<T> customizeInformer(String name, SharedIndexInformer<T> informer) {
        return informer;
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
                        apiClient,
                        titusRuntime
                );

                this.nodeInformer = customizeInformer("nodeInformer", createNodeInformer(sharedInformerFactory));
                this.podInformer = customizeInformer("podInformer", createPodInformer(sharedInformerFactory));
                this.persistentVolumeInformer = customizeInformer("persistentVolumeInformer", createPersistentVolumeInformer(sharedInformerFactory));
                this.persistentVolumeClaimInformer = customizeInformer("persistentVolumeClaimInformer", createPersistentVolumeClaimInformer(sharedInformerFactory));

                this.nodeInformerMetrics = new StdKubeInformerMetrics<>("node", nodeInformer, titusRuntime);
                this.podInformerMetrics = new StdKubeInformerMetrics<>("pod", podInformer, titusRuntime);
                this.persistentVolumeInformerMetrics = new StdKubeInformerMetrics<>("persistentvolume", persistentVolumeInformer, titusRuntime);
                this.persistentVolumeClaimInformerMetrics = new StdKubeInformerMetrics<>("persistentvolumeclaim", persistentVolumeClaimInformer, titusRuntime);

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

    private SharedIndexInformer<V1Node> createNodeInformer(SharedInformerFactory sharedInformerFactory) {
        return sharedInformerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> coreV1Api.listNodeCall(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        params.resourceVersion,
                        null,
                        params.timeoutSeconds,
                        params.watch,
                        null
                ),
                V1Node.class,
                V1NodeList.class,
                configuration.getKubeApiServerIntegratorRefreshIntervalMs()
        );
    }

    private SharedIndexInformer<V1Pod> createPodInformer(SharedInformerFactory sharedInformerFactory) {
        return sharedInformerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> coreV1Api.listNamespacedPodCall(
                        KUBERNETES_NAMESPACE,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        params.resourceVersion,
                        null,
                        params.timeoutSeconds,
                        params.watch,
                        null
                ),
                V1Pod.class,
                V1PodList.class,
                configuration.getKubeApiServerIntegratorRefreshIntervalMs()
        );
    }

    private SharedIndexInformer<V1PersistentVolume> createPersistentVolumeInformer(SharedInformerFactory sharedInformerFactory) {
        return sharedInformerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> coreV1Api.listPersistentVolumeCall(
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        params.resourceVersion,
                        null,
                        params.timeoutSeconds,
                        params.watch,
                        null
                ),
                V1PersistentVolume.class,
                V1PersistentVolumeList.class,
                configuration.getKubeApiServerIntegratorRefreshIntervalMs()
        );
    }

    private SharedIndexInformer<V1PersistentVolumeClaim> createPersistentVolumeClaimInformer(SharedInformerFactory sharedInformerFactory) {
        return sharedInformerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> coreV1Api.listNamespacedPersistentVolumeClaimCall(
                        KUBERNETES_NAMESPACE,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        params.resourceVersion,
                        null,
                        params.timeoutSeconds,
                        params.watch,
                        null
                ),
                V1PersistentVolumeClaim.class,
                V1PersistentVolumeClaimList.class,
                configuration.getKubeApiServerIntegratorRefreshIntervalMs()
        );
    }
}
