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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.util.CallGeneratorParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.master.mesos.kubeapiserver.KubeUtil.createSharedInformerFactory;

@Singleton
public class DefaultDirectKubeApiServerIntegrator implements DirectKubeApiServerIntegrator {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDirectKubeApiServerIntegrator.class);

    private static final String KUBERNETES_NAMESPACE = "default";

    private final DirectKubeConfiguration configuration;

    private final ApiClient apiClient;
    private final CoreV1Api coreV1Api;
    private final TaskToPodConverter taskToPodConverter;

    private volatile SharedInformerFactory sharedInformerFactory;
    private volatile SharedIndexInformer<V1Pod> podInformer;

    private final ConcurrentMap<String, V1Pod> pods = new ConcurrentHashMap<>();

    private final ExecutorService apiClientExecutor;
    private final Scheduler apiClientScheduler;

    @Inject
    public DefaultDirectKubeApiServerIntegrator(DirectKubeConfiguration configuration,
                                                ApiClient apiClient,
                                                TaskToPodConverter taskToPodConverter,
                                                TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.apiClient = apiClient;
        this.coreV1Api = new CoreV1Api(apiClient);
        this.taskToPodConverter = taskToPodConverter;

        this.apiClientExecutor = ExecutorsExt.instrumentedFixedSizeThreadPool(titusRuntime.getRegistry(), "kube-apiclient", configuration.getApiClientThreadPoolSize());
        this.apiClientScheduler = Schedulers.fromExecutorService(apiClientExecutor);
    }

    @Activator
    public void enterActiveMode() {
        this.sharedInformerFactory = createSharedInformerFactory(
                "kube-api-server-integrator-shared-informer-",
                apiClient
        );
        this.podInformer = createPodInformer(sharedInformerFactory, coreV1Api);
        sharedInformerFactory.startAllRegisteredInformers();
    }

    @PreDestroy
    public void shutdown() {
        if (sharedInformerFactory != null) {
            sharedInformerFactory.stopAllRegisteredInformers();
        }
        apiClientScheduler.dispose();
        apiClientExecutor.shutdown();
    }

    @Override
    public Map<String, V1Pod> getPods() {
        return new HashMap<>(pods);
    }

    @Override
    public Mono<V1Pod> launchTask(Job job, Task task) {
        return Mono.defer(() -> {
            try {
                V1Pod v1Pod = taskToPodConverter.apply(job, task);
                logger.info("creating pod: {}", v1Pod);
                coreV1Api.createNamespacedPod(KUBERNETES_NAMESPACE, v1Pod, null, null, null);
                pods.putIfAbsent(task.getId(), v1Pod);
                return Mono.just(v1Pod);
            } catch (ApiException e) {
                logger.error("Unable to create pod with error:", e);
                return Mono.error(new IllegalStateException("Unable to launch a task " + task.getId(), e));
            }
        }).subscribeOn(apiClientScheduler).timeout(Duration.ofMillis(configuration.getKubeApiClientTimeoutMs()));
    }

    @Override
    public Mono<Void> terminateTask(String taskId) {
        return Mono.error(new IllegalStateException("not implemented yet"));
    }

    @Override
    public Flux<PodEvent> events() {
        return Flux.create(sink -> {
            if (podInformer == null) {
                sink.error(new IllegalStateException("Service not activated yet"));
                return;
            }

            ResourceEventHandler<V1Pod> handler = new ResourceEventHandler<V1Pod>() {
                @Override
                public void onAdd(V1Pod pod) {
                    logger.info("Pod Added: {}", pod);
                    String taskId = pod.getSpec().getContainers().get(0).getName();

                    V1Pod old = pods.get(taskId);
                    pods.put(taskId, pod);

                    if (old != null) {
                        sink.next(PodEvent.onUpdate(old, pod));
                    } else {
                        sink.next(PodEvent.onAdd(pod));
                    }
                }

                @Override
                public void onUpdate(V1Pod oldPod, V1Pod newPod) {
                    logger.info("Pod Updated Old: {}, New: {}", oldPod, newPod);
                    pods.put(newPod.getSpec().getContainers().get(0).getName(), newPod);
                    sink.next(PodEvent.onUpdate(oldPod, newPod));
                }

                @Override
                public void onDelete(V1Pod pod, boolean deletedFinalStateUnknown) {
                    logger.info("Pod Deleted: {}, deletedFinalStateUnknown={}", pod, deletedFinalStateUnknown);
                    pods.remove(pod.getSpec().getContainers().get(0).getName());
                    sink.next(PodEvent.onDelete(pod, deletedFinalStateUnknown));
                }
            };
            podInformer.addEventHandler(handler);

            // A listener cannot be removed from shared informer.
            // sink.onCancel(() -> ???);
        });
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
