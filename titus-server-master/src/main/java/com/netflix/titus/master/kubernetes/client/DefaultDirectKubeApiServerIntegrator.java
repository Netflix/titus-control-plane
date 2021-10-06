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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
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
import com.netflix.titus.api.jobmanager.model.job.ContainerHealth;
import com.netflix.titus.api.jobmanager.model.job.ContainerState;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolume;
import com.netflix.titus.api.jobmanager.model.job.ebs.EbsVolumeUtils;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.master.kubernetes.client.model.PodDeletedEvent;
import com.netflix.titus.master.kubernetes.client.model.PodEvent;
import com.netflix.titus.master.kubernetes.client.model.PodUpdatedEvent;
import com.netflix.titus.master.kubernetes.pod.PodFactory;
import com.netflix.titus.master.kubernetes.KubeUtil;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiException;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Volume;
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
    private static final int DELETE_GRACE_PERIOD_SECONDS = 300;

    private static final String NOT_FOUND = "Not Found";

    private final DirectKubeConfiguration configuration;
    private final KubeApiFacade kubeApiFacade;

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
                                                KubeApiFacade kubeApiFacade,
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

    public List<ContainerState> getPodStatus(String taskId) {
        if(pods.get(taskId) == null) {
            return Collections.emptyList();
        }

        List<V1ContainerStatus> v1ContainerStatus = pods.get(taskId).getStatus().getContainerStatuses();
        ArrayList<ContainerState> containerstates = new ArrayList();
        if (v1ContainerStatus.isEmpty() || v1ContainerStatus.size() == 0) {
            // we have pod status but no container status just yet
            return Collections.emptyList();
        } else {
            ListIterator<V1ContainerStatus> iterator = v1ContainerStatus.listIterator();
            while (iterator.hasNext()) {
                V1ContainerStatus v1ContainerStatus1 = iterator.next();
                V1ContainerState status = v1ContainerStatus1.getState();
                ContainerHealth health = ContainerHealth.Unset;
                if (V1ContainerState.SERIALIZED_NAME_RUNNING.equals(status)) {
                    health = ContainerHealth.Healthy;
                } else if (V1ContainerState.SERIALIZED_NAME_TERMINATED.equals(status)) {
                    health = ContainerHealth.Unhealthy;
                } else if (V1ContainerState.SERIALIZED_NAME_WAITING.equals(status)) {
                    health = ContainerHealth.Unset;
                }
                ContainerState containerState =
                        ContainerState.newBuilder()
                                .withContainerName(v1ContainerStatus1.getName())
                                .withContainerHealth(health)
                                .build();
                containerstates.add(containerState);
            }
            return containerstates;
        }
    }

    @Override
    public Mono<Void> launchTask(Job job, Task task) {
        boolean isEbsVolumePvEnabled = configuration.isEbsVolumePvEnabled();
        return Mono.fromCallable(() -> {
            try {
                V1Pod v1Pod = podFactory.buildV1Pod(job, task, true, isEbsVolumePvEnabled);
                logger.info("creating pod: {}", formatPodEssentials(v1Pod));
                logger.debug("complete pod data: {}", v1Pod);
                return v1Pod;
            } catch (Exception e) {
                logger.error("Unable to convert job {} and task {} to pod: {}", job, task, KubeUtil.toErrorDetails(e), e);
                throw new IllegalStateException("Unable to convert task to pod " + task.getId(), e);
            }
        })
                .flatMap(v1Pod -> isEbsVolumePvEnabled
                        ? launchEbsVolume(job, task, v1Pod).then(Mono.just(v1Pod))
                        : Mono.just(v1Pod))
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

    /**
     * Launches/creates any EBS persistent volumes or claims associated with the task/pod.
     */
    private Mono<Void> launchEbsVolume(Job<?> job, Task task, V1Pod v1Pod) {
        // We currently only handle a single volume per-pod
        Optional<V1Volume> optionalV1Volume = CollectionsExt.nonNull(v1Pod.getSpec().getVolumes())
                .stream()
                .filter(v1Volume -> null != v1Volume.getPersistentVolumeClaim())
                .findFirst();
        if (!optionalV1Volume.isPresent()) {
            logger.info("No v1 volume claim found for job {} task {}", job.getId(), task.getId());
            return Mono.empty();
        }
        V1Volume v1Volume = optionalV1Volume.get();

        // Expect to find an EBS volume from the job/task that matches the pod's volume
        Optional<EbsVolume> optionalEbsVolume = EbsVolumeUtils.getEbsVolumeForTask(job, task);
        if (!optionalEbsVolume.isPresent()) {
            throw new IllegalStateException(String.format("Expected EBS volume for job %s and task %s", job, task));
        }
        EbsVolume ebsVolume = optionalEbsVolume.get();
        if (!ebsVolume.getVolumeId().equals(v1Volume.getName())) {
            throw new IllegalStateException(String.format("Pod %s volume name %s does not match task %s volume id %s", v1Pod, v1Volume.getName(), task, ebsVolume.getVolumeId()));
        }

        // Create a persistent volume followed by a claim for that volume
        // If the attempt to create the PVC fails, the PV will be subsequently garbage collected
        return launchEbsPersistentVolume(ebsVolume)
                .flatMap(v1PersistentVolume -> launchPersistentVolumeClaim(v1PersistentVolume, v1Pod))
                .then();
    }

    private Mono<V1PersistentVolume> launchEbsPersistentVolume(EbsVolume ebsVolume) {
        return Mono.defer(() -> {
            Stopwatch timer = Stopwatch.createStarted();

            V1PersistentVolume v1PersistentVolume = KubeModelConverters.toEbsV1PersistentVolume(ebsVolume);
            logger.info("Creating persistent volume {}", v1PersistentVolume);

            try {
                kubeApiFacade.createPersistentVolume(v1PersistentVolume);
                logger.info("Created persistent volume {} in {}ms", v1PersistentVolume, timer.elapsed(TimeUnit.MILLISECONDS));
                metrics.persistentVolumeCreateSuccess(timer.elapsed(TimeUnit.MILLISECONDS));
            } catch (KubeApiException apiException) {
                if (isEbsVolumeConflictException(apiException)) {
                    logger.info("Persistent volume already exists {}", v1PersistentVolume);
                } else {
                    logger.error("Unable to create persistent volume {}, error: {}", v1PersistentVolume, apiException);
                    metrics.persistentVolumeCreateError(apiException, timer.elapsed(TimeUnit.MILLISECONDS));
                    return Mono.error(new IllegalStateException("Unable to create a persistent volume " + v1PersistentVolume, apiException));
                }
            }

            return Mono.just(v1PersistentVolume);
        });
    }

    private Mono<V1PersistentVolumeClaim> launchPersistentVolumeClaim(V1PersistentVolume v1PersistentVolume, V1Pod v1Pod) {
        return Mono.defer(() -> {
            Stopwatch timer = Stopwatch.createStarted();

            V1PersistentVolumeClaim v1PersistentVolumeClaim = KubeModelConverters.toV1PersistentVolumeClaim(v1PersistentVolume, v1Pod);
            logger.info("Creating persistent volume claim {}", v1PersistentVolumeClaim);

            try {
                kubeApiFacade.createNamespacedPersistentVolumeClaim(KUBERNETES_NAMESPACE, v1PersistentVolumeClaim);
                long latencyMs = timer.elapsed(TimeUnit.MILLISECONDS);
                logger.info("Created persistent volume claim {} in {}ms", v1PersistentVolumeClaim, latencyMs);
                metrics.persistentVolumeClaimCreateSuccess(latencyMs);
            } catch (KubeApiException apiException) {
                if (isEbsVolumeConflictException(apiException)) {
                    logger.info("Persistent volume claim already exists {}", v1PersistentVolumeClaim);
                } else {
                    logger.error("Unable to create persistent volume claim {}, {}, error: {}", v1PersistentVolumeClaim, apiException.getCause(), apiException);
                    metrics.persistentVolumeClaimCreateError(apiException, timer.elapsed(TimeUnit.MILLISECONDS));
                    return Mono.error(new IllegalStateException("Unable to create a persistent volume claim " + v1PersistentVolumeClaim, apiException));
                }
            }

            return Mono.just(v1PersistentVolumeClaim);
        });
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
