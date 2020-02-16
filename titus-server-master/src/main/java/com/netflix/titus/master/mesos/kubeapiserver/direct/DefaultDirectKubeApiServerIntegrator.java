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

import com.google.gson.JsonSyntaxException;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.master.mesos.kubeapiserver.direct.model.PodEvent;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Singleton
public class DefaultDirectKubeApiServerIntegrator implements DirectKubeApiServerIntegrator {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDirectKubeApiServerIntegrator.class);

    private static final String KUBERNETES_NAMESPACE = "default";
    private static final int DELETE_GRACE_PERIOD_SECONDS = 300;

    private static final String NOT_FOUND = "Not Found";

    private final DirectKubeConfiguration configuration;
    private final KubeApiFacade kubeApiFacade;

    private final TaskToPodConverter taskToPodConverter;

    private final DirectProcessor<PodEvent> supplementaryPodEventProcessor = DirectProcessor.create();

    /**
     * We use {@link FluxSink.OverflowStrategy#LATEST} with the assumption that the supplementary events created here, will
     * be re-crated later if needed (like pod not found event).
     */
    private final FluxSink<PodEvent> supplementaryPodEventSink = supplementaryPodEventProcessor.sink(FluxSink.OverflowStrategy.LATEST);

    private final ConcurrentMap<String, V1Pod> pods = new ConcurrentHashMap<>();

    private final ExecutorService apiClientExecutor;
    private final Scheduler apiClientScheduler;

    @Inject
    public DefaultDirectKubeApiServerIntegrator(DirectKubeConfiguration configuration,
                                                KubeApiFacade kubeApiFacade,
                                                TaskToPodConverter taskToPodConverter,
                                                TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.kubeApiFacade = kubeApiFacade;
        this.taskToPodConverter = taskToPodConverter;

        this.apiClientExecutor = ExecutorsExt.instrumentedFixedSizeThreadPool(titusRuntime.getRegistry(), "kube-apiclient", configuration.getApiClientThreadPoolSize());
        this.apiClientScheduler = Schedulers.fromExecutorService(apiClientExecutor);
    }

    @PreDestroy
    public void shutdown() {
        apiClientScheduler.dispose();
        apiClientExecutor.shutdown();
    }

    @Override
    public Map<String, V1Pod> getPods() {
        return new HashMap<>(pods);
    }

    @Override
    public Mono<V1Pod> launchTask(Job job, Task task) {
        return Mono.fromCallable(() -> {
            try {
                V1Pod v1Pod = taskToPodConverter.apply(job, task);
                logger.info("creating pod: {}", v1Pod);
                kubeApiFacade.getCoreV1Api().createNamespacedPod(KUBERNETES_NAMESPACE, v1Pod, null, null, null);
                pods.putIfAbsent(task.getId(), v1Pod);
                return v1Pod;
            } catch (ApiException e) {
                logger.error("Unable to create pod with error:", e);
                throw new IllegalStateException("Unable to launch a task " + task.getId(), e);
            }
        }).subscribeOn(apiClientScheduler).timeout(Duration.ofMillis(configuration.getKubeApiClientTimeoutMs()));
    }

    @Override
    public Mono<Void> terminateTask(Task task) {
        String taskId = task.getId();

        return Mono.<Void>fromRunnable(() -> {
            try {
                logger.info("Deleting pod: {}", taskId);
                kubeApiFacade.getCoreV1Api().deleteNamespacedPod(taskId, KUBERNETES_NAMESPACE, null, null, null, DELETE_GRACE_PERIOD_SECONDS, null, null);
            } catch (JsonSyntaxException e) {
                // this is probably successful. the generated client has the wrong response type
            } catch (ApiException e) {
                if (e.getMessage().equalsIgnoreCase(NOT_FOUND) && task.getStatus().getState() == TaskState.Accepted) {
                    sendEvent(PodEvent.onPodNotFound(task));
                } else {
                    logger.error("Failed to kill task: {} with error: ", taskId, e);
                }
            } catch (Exception e) {
                logger.error("Failed to kill task: {} with error: ", taskId, e);
            }
        }).subscribeOn(apiClientScheduler).timeout(Duration.ofMillis(configuration.getKubeApiClientTimeoutMs()));
    }

    @Override
    public Flux<PodEvent> events() {
        return kubeInformerEvents().mergeWith(supplementaryPodEventProcessor).compose(ReactorExt.badSubscriberHandler(logger));
    }

    private Flux<PodEvent> kubeInformerEvents() {
        return Flux.create(sink -> {
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
            kubeApiFacade.getPodInformer().addEventHandler(handler);

            // A listener cannot be removed from shared informer.
            // sink.onCancel(() -> ???);
        });
    }

    private void sendEvent(PodEvent podEvent) {
        supplementaryPodEventSink.next(podEvent);
    }
}
