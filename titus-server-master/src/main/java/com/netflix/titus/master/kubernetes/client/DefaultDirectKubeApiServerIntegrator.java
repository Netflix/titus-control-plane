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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Stopwatch;
import com.google.gson.JsonSyntaxException;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.master.kubernetes.KubeUtil;
import com.netflix.titus.master.kubernetes.client.model.PodDeletedEvent;
import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import com.netflix.titus.master.kubernetes.client.model.PodUpdatedEvent;
import com.netflix.titus.master.kubernetes.pod.PodFactory;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiException;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static com.netflix.titus.master.kubernetes.KubeObjectFormatter.formatPodEssentials;

@Singleton
public class DefaultDirectKubeApiServerIntegrator implements DirectKubeApiServerIntegrator {

    private static final Logger logger = LoggerFactory.getLogger(DefaultDirectKubeApiServerIntegrator.class);

    private static final String KUBERNETES_NAMESPACE = "default";
    /**
     * This setting needs to be higher than the gracePeriodSeconds on the pod spec itself.
     * This is because we need to give VK its own gracePeriodSeconds time to actually shut down,
     * *before* TJC/kube actually delete the pod from api server, because other related systems
     * need to still have the pod object in memory. If we delete the pod object too soon, we may
     * incorrectly assume that the pod's resource (e.g. ip addreses) are free to use!
     * */
    private static final int DELETE_GRACE_PERIOD_SECONDS = 800;

    private static final String NOT_FOUND = "Not Found";

    private final DirectKubeConfiguration configuration;
    private final StdKubeApiFacade kubeApiFacade;

    private final PodFactory podFactory;
    private final DefaultDirectKubeApiServerIntegratorMetrics metrics;

    private final PodCreateErrorToResultCodeResolver podCreateErrorToReasonCodeResolver;

    private final TitusRuntime titusRuntime;

    private final DirectProcessor<PodEvent> supplementaryPodEventProcessor = DirectProcessor.create();

    /**
     * We use {@link FluxSink.OverflowStrategy#LATEST} with the assumption that the supplementary events created here, will
     * be re-crated later if needed (like pod not found event).
     */
    private final FluxSink<PodEvent> supplementaryPodEventSink = supplementaryPodEventProcessor.sink(FluxSink.OverflowStrategy.LATEST);

    private final ConcurrentMap<String, V1Pod> pods = new ConcurrentHashMap<>();

    private final ExecutorService apiClientExecutor;
    private final Scheduler apiClientScheduler;

    private final Optional<FitInjection> fitKubeInjection;

    @Inject
    public DefaultDirectKubeApiServerIntegrator(DirectKubeConfiguration configuration,
                                                StdKubeApiFacade kubeApiFacade,
                                                PodFactory podFactory,
                                                TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.kubeApiFacade = kubeApiFacade;
        this.podFactory = podFactory;
        this.podCreateErrorToReasonCodeResolver = new PodCreateErrorToResultCodeResolver(configuration);
        this.titusRuntime = titusRuntime;

        this.metrics = new DefaultDirectKubeApiServerIntegratorMetrics(titusRuntime);
        metrics.observePodsCollection(pods);

        this.apiClientExecutor = ExecutorsExt.instrumentedFixedSizeThreadPool(titusRuntime.getRegistry(), "kube-apiclient", configuration.getApiClientThreadPoolSize());
        this.apiClientScheduler = Schedulers.fromExecutorService(apiClientExecutor);

        FitFramework fit = titusRuntime.getFitFramework();
        if (fit.isActive()) {
            FitInjection fitKubeInjection = fit.newFitInjectionBuilder("directKubeIntegration")
                    .withDescription("DefaultDirectKubeApiServerIntegrator injection")
                    .build();
            fit.getRootComponent().getChild(DirectKubeApiServerIntegrator.COMPONENT).addInjection(fitKubeInjection);
            this.fitKubeInjection = Optional.of(fitKubeInjection);
        } else {
            this.fitKubeInjection = Optional.empty();
        }
    }

    @PreDestroy
    public void shutdown() {
        apiClientScheduler.dispose();
        apiClientExecutor.shutdown();
        metrics.shutdown();
    }

    @Override
    public Map<String, V1Pod> getPods() {
        return new HashMap<>(pods);
    }

    @Override
    public boolean isReadyForScheduling() {
        return kubeApiFacade.isReadyForScheduling();
    }

    @Override
    public Mono<Void> launchTask(Job job, Task task) {
        return Mono.fromCallable(() -> {
                    try {
                        V1Pod v1Pod = podFactory.buildV1Pod(job, task);
                        logger.info("creating pod: {}", formatPodEssentials(v1Pod));
                        logger.debug("complete pod data: {}", v1Pod);
                        return v1Pod;
                    } catch (Exception e) {
                        logger.error("Unable to convert job {} and task {} to pod: {}", job, task, KubeUtil.toErrorDetails(e), e);
                        throw new IllegalStateException("Unable to convert task to pod " + task.getId(), e);
                    }
                })
                .flatMap(v1Pod -> launchPod(task, v1Pod))
                .subscribeOn(apiClientScheduler)
                .timeout(Duration.ofMillis(configuration.getKubeApiClientTimeoutMs()))
                .doOnError(TimeoutException.class, e -> metrics.launchTimeout(configuration.getKubeApiClientTimeoutMs()))
                .ignoreElement()
                .cast(Void.class);
    }

    @Override
    public Mono<Void> terminateTask(Task task) {
        String taskId = task.getId();

        return Mono.<Void>fromRunnable(() -> {
            Stopwatch timer = Stopwatch.createStarted();
            try {
                logger.info("Deleting pod: {}", taskId);
                kubeApiFacade.deleteNamespacedPod(KUBERNETES_NAMESPACE, taskId, DELETE_GRACE_PERIOD_SECONDS);
                metrics.terminateSuccess(task, timer.elapsed(TimeUnit.MILLISECONDS));
            } catch (JsonSyntaxException e) {
                // this is probably successful. the generated client has the wrong response type
                metrics.terminateSuccess(task, timer.elapsed(TimeUnit.MILLISECONDS));
            } catch (KubeApiException e) {
                metrics.terminateError(task, e, timer.elapsed(TimeUnit.MILLISECONDS));

                if (e.getErrorCode() == KubeApiException.ErrorCode.NOT_FOUND && task.getStatus().getState() == TaskState.Accepted) {
                    sendEvent(PodEvent.onPodNotFound(task,
                            TaskStatus.newBuilder()
                                    .withState(TaskState.Finished)
                                    .withReasonCode(TaskStatus.REASON_TASK_LOST)
                                    .withReasonMessage("Task terminate requested, but its container is not found")
                                    .withTimestamp(titusRuntime.getClock().wallTime())
                                    .build()
                    ));
                } else {
                    logger.error("Failed to kill task: {} with error: {}", taskId, KubeUtil.toErrorDetails(e), e);
                }
            } catch (Exception e) {
                logger.error("Failed to kill task: {} with error: {}", taskId, KubeUtil.toErrorDetails(e), e);
                metrics.terminateError(task, e, timer.elapsed(TimeUnit.MILLISECONDS));
            }
        }).subscribeOn(apiClientScheduler).timeout(Duration.ofMillis(configuration.getKubeApiClientTimeoutMs()));
    }

    @Override
    public Flux<PodEvent> events() {
        return kubeInformerEvents().mergeWith(supplementaryPodEventProcessor).transformDeferred(ReactorExt.badSubscriberHandler(logger));
    }

    @Override
    public String resolveReasonCode(Throwable cause) {
        return podCreateErrorToReasonCodeResolver.resolveReasonCode(cause);
    }

    private Mono<V1Pod> launchPod(Task task, V1Pod v1Pod) {
        return Mono.fromCallable(() -> {
            Stopwatch timer = Stopwatch.createStarted();
            try {
                fitKubeInjection.ifPresent(i -> i.beforeImmediate(KubeFitAction.ErrorKind.POD_CREATE_ERROR.name()));

                kubeApiFacade.createNamespacedPod(KUBERNETES_NAMESPACE, v1Pod);
                pods.putIfAbsent(task.getId(), v1Pod);

                metrics.launchSuccess(task, v1Pod, timer.elapsed(TimeUnit.MILLISECONDS));

                return v1Pod;
            } catch (Exception e) {
                logger.error("Unable to create pod with error: {}", KubeUtil.toErrorDetails(e), e);

                metrics.launchError(task, e, timer.elapsed(TimeUnit.MILLISECONDS));

                throw new IllegalStateException("Unable to launch a task " + task.getId(), e);
            }
        });
    }

    private Flux<PodEvent> kubeInformerEvents() {
        return Flux.create(sink -> {
            ResourceEventHandler<V1Pod> handler = new ResourceEventHandler<V1Pod>() {
                @Override
                public void onAdd(V1Pod pod) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        if (!KubeUtil.isOwnedByKubeScheduler(pod)) {
                            return;
                        }
                        String taskId = pod.getSpec().getContainers().get(0).getName();

                        V1Pod old = pods.get(taskId);
                        pods.put(taskId, pod);

                        PodEvent podEvent;
                        if (old != null) {
                            podEvent = PodEvent.onUpdate(old, pod, findNode(pod));
                            metrics.onUpdate(pod);
                        } else {
                            podEvent = PodEvent.onAdd(pod);
                            metrics.onAdd(pod);
                        }
                        sink.next(podEvent);

                        logger.info("Pod Added: pod={}, sequenceNumber={}", formatPodEssentials(pod), podEvent.getSequenceNumber());
                        logger.debug("complete pod data: {}", pod);
                    } finally {
                        logger.info("Pod informer onAdd: pod={}, elapsedMs={}", pod.getMetadata().getName(), stopwatch.elapsed().toMillis());
                    }
                }

                @Override
                public void onUpdate(V1Pod oldPod, V1Pod newPod) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        if (!KubeUtil.isOwnedByKubeScheduler(newPod)) {
                            return;
                        }

                        metrics.onUpdate(newPod);

                        pods.put(newPod.getSpec().getContainers().get(0).getName(), newPod);

                        PodUpdatedEvent podEvent = PodEvent.onUpdate(oldPod, newPod, findNode(newPod));
                        sink.next(podEvent);

                        logger.info("Pod Updated: old={}, new={}, sequenceNumber={}", formatPodEssentials(oldPod), formatPodEssentials(newPod), podEvent.getSequenceNumber());
                        logger.debug("Complete pod data: old={}, new={}", oldPod, newPod);
                    } finally {
                        logger.info("Pod informer onUpdate: pod={}, elapsedMs={}", newPod.getMetadata().getName(), stopwatch.elapsed().toMillis());
                    }
                }

                @Override
                public void onDelete(V1Pod pod, boolean deletedFinalStateUnknown) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        if (!KubeUtil.isOwnedByKubeScheduler(pod)) {
                            return;
                        }

                        metrics.onDelete(pod);

                        pods.remove(pod.getSpec().getContainers().get(0).getName());

                        PodDeletedEvent podEvent = PodEvent.onDelete(pod, deletedFinalStateUnknown, findNode(pod));
                        sink.next(podEvent);

                        logger.info("Pod Deleted: {}, deletedFinalStateUnknown={}, sequenceNumber={}", formatPodEssentials(pod), deletedFinalStateUnknown, podEvent.getSequenceNumber());
                        logger.debug("complete pod data: {}", pod);
                    } finally {
                        logger.info("Pod informer onDelete: pod={}, elapsedMs={}", pod.getMetadata().getName(), stopwatch.elapsed().toMillis());
                    }
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

    private Optional<V1Node> findNode(V1Pod pod) {
        String nodeName = pod.getSpec().getNodeName();
        if (StringExt.isEmpty(nodeName)) {
            return Optional.empty();
        }
        return Optional.ofNullable(kubeApiFacade.getNodeInformer().getIndexer().getByKey(nodeName));
    }

    private static boolean isEbsVolumeConflictException(KubeApiException apiException) {
        return apiException.getErrorCode() == KubeApiException.ErrorCode.CONFLICT_ALREADY_EXISTS;
    }
}
