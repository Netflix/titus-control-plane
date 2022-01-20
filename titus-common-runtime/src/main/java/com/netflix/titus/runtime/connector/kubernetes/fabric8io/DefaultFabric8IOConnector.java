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

package com.netflix.titus.runtime.connector.kubernetes.fabric8io;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.model.PodDeletedEvent;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.model.PodEvent;
import com.netflix.titus.runtime.connector.kubernetes.fabric8io.model.PodUpdatedEvent;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import static com.netflix.titus.runtime.connector.kubernetes.fabric8io.model.F8KubeObjectFormatter.formatPodEssentials;

@Singleton
public class DefaultFabric8IOConnector implements Fabric8IOConnector {
    private static final Logger logger = LoggerFactory.getLogger("KubeSharedInformerLogger");
    private final NamespacedKubernetesClient kubernetesClient;
    private final TitusRuntime titusRuntime;
    private volatile PMap<String, Pod> pods = HashTreePMap.empty();
    private volatile SharedInformerFactory sharedInformerFactory;
    private volatile SharedIndexInformer<Pod> podInformer;
    private volatile SharedIndexInformer<Node> nodeInformer;

    private ExecutorService notificationHandlerExecutor;
    private Scheduler scheduler;
    private Disposable subscription;

    @Inject
    public DefaultFabric8IOConnector(NamespacedKubernetesClient kubernetesClient, TitusRuntime titusRuntime) {
        this.kubernetesClient = kubernetesClient;
        this.titusRuntime = titusRuntime;
        enterActiveMode();
    }

    private final Object activationLock = new Object();
    private volatile boolean deactivated;

    @PreDestroy
    public void shutdown() {
        if (sharedInformerFactory != null) {
            sharedInformerFactory.stopAllRegisteredInformers();
        }
    }

    @Activator
    public void enterActiveMode() {
        logger.info("Kube api connector entering active mode");
        this.scheduler = initializeNotificationScheduler();
        AtomicLong pendingCounter = new AtomicLong();
        this.subscription = this.events().subscribeOn(scheduler)
                .publishOn(scheduler)
                .doOnError(error -> logger.warn("Kube integration event stream terminated with an error (retrying soon)", error))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)))
                .subscribe(
                        event -> {
                            Stopwatch stopwatch = Stopwatch.createStarted();
                            pendingCounter.getAndIncrement();
                            logger.info("New event [pending={}, lag={}]: {}", pendingCounter.get(), PodEvent.nextSequence() - event.getSequenceNumber(), event);
                            processEvent(event)
                                    .doAfterTerminate(() -> {
                                        pendingCounter.decrementAndGet();
                                        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                                        logger.info("Event processed [pending={}]: event={}, elapsed={}", pendingCounter.get(), event, elapsed);
                                    })
                                    .subscribe(
                                            next -> {
                                                // nothing
                                            },
                                            error -> {
                                                logger.info("Kube api connector event state update error: event={}, error={}", event, error.getMessage());
                                                logger.debug("Stack trace", error);
                                            },
                                            () -> {
                                                // nothing
                                            }
                                    );
                        },
                        e -> logger.error("Event stream terminated"),
                        () -> logger.info("Event stream completed")
                );
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
    public Map<String, Pod> getPods() {
        return pods;
    }

    @Override
    public SharedIndexInformer<Pod> getPodInformer() {
        activate();
        return podInformer;
    }

    @Override
    public SharedIndexInformer<Node> getNodeInformer() {
        activate();
        return nodeInformer;
    }

    @Override
    public Flux<PodEvent> events() {
        return kubeInformerEvents().transformDeferred(ReactorExt.badSubscriberHandler(logger));
    }

    private void activate() {
        synchronized (activationLock) {
            if (sharedInformerFactory != null) {
                return;
            }
            try {
                this.sharedInformerFactory = kubernetesClient.informers();
                this.podInformer = createPodInformer(sharedInformerFactory);
                this.nodeInformer = createNodeInformer(sharedInformerFactory);
                sharedInformerFactory.startAllRegisteredInformers();
                logger.info("Kube pod informer activated");
            } catch (Exception e) {
                logger.error("Could not intialize kubernetes shared informer", e);
                if (sharedInformerFactory != null) {
                    ExceptionExt.silent(() -> sharedInformerFactory.stopAllRegisteredInformers());
                }
                sharedInformerFactory = null;
                podInformer = null;
            }
        }
    }

    private Optional<Node> findNode(Pod pod) {
        String nodeName = pod.getSpec().getNodeName();
        if (StringExt.isEmpty(nodeName)) {
            return Optional.empty();
        }
        return Optional.ofNullable(this.getNodeInformer().getIndexer().getByKey(nodeName));
    }


    private Flux<PodEvent> kubeInformerEvents() {
        return Flux.create(sink -> {
            ResourceEventHandler<Pod> handler = new ResourceEventHandler<Pod>() {
                @Override
                public void onAdd(Pod pod) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        String taskId = pod.getMetadata().getName();

                        Pod old = pods.get(taskId);
                        pods = pods.plus(taskId, pod);

                        PodEvent podEvent;
                        if (old != null) {
                            podEvent = PodEvent.onUpdate(old, pod, findNode(pod));
                            //metrics.onUpdate(pod);
                        } else {
                            podEvent = PodEvent.onAdd(pod);
                            //metrics.onAdd(pod);
                        }
                        sink.next(podEvent);

                        logger.info("Pod Added: pod={}, sequenceNumber={}", formatPodEssentials(pod), podEvent.getSequenceNumber());
                        logger.debug("complete pod data: {}", pod);
                    } finally {
                        logger.info("Pod informer onAdd: pod={}, elapsedMs={}", pod.getMetadata().getName(), stopwatch.elapsed().toMillis());
                    }
                }

                @Override
                public void onUpdate(Pod oldPod, Pod newPod) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        String taskId = newPod.getMetadata().getName();
                        pods = pods.plus(taskId, newPod);

                        PodUpdatedEvent podEvent = PodEvent.onUpdate(oldPod, newPod, findNode(newPod));
                        sink.next(podEvent);

                        logger.info("Pod Updated: old={}, new={}, sequenceNumber={}", formatPodEssentials(oldPod), formatPodEssentials(newPod), podEvent.getSequenceNumber());
                        logger.debug("Complete pod data: old={}, new={}", oldPod, newPod);
                    } finally {
                        logger.info("Pod informer onUpdate: pod={}, elapsedMs={}", newPod.getMetadata().getName(), stopwatch.elapsed().toMillis());
                    }
                }

                @Override
                public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        String taskId = pod.getMetadata().getName();
                        pods = pods.minus(taskId);

                        PodDeletedEvent podEvent = PodEvent.onDelete(pod, deletedFinalStateUnknown, findNode(pod));
                        sink.next(podEvent);

                        logger.info("Pod Deleted: {}, deletedFinalStateUnknown={}, sequenceNumber={}", formatPodEssentials(pod), deletedFinalStateUnknown, podEvent.getSequenceNumber());
                        logger.debug("complete pod data: {}", pod);
                    } finally {
                        logger.info("Pod informer onDelete: pod={}, elapsedMs={}", pod.getMetadata().getName(), stopwatch.elapsed().toMillis());
                    }
                }
            };
            this.getPodInformer().addEventHandler(handler);

            // A listener cannot be removed from shared informer.
            // sink.onCancel(() -> ???);
        });
    }

    private Mono<Void> processEvent(PodEvent event) {
        // TODO drop if not needed
        return Mono.empty();
    }

    @VisibleForTesting
    protected Scheduler initializeNotificationScheduler() {
        this.notificationHandlerExecutor = ExecutorsExt.namedSingleThreadExecutor(DefaultFabric8IOConnector.class.getSimpleName());
        return Schedulers.fromExecutor(notificationHandlerExecutor);
    }

    private SharedIndexInformer<Pod> createPodInformer(SharedInformerFactory sharedInformerFactory) {
        return sharedInformerFactory.sharedIndexInformerFor(
                Pod.class,
                3000);
    }

    private SharedIndexInformer<Node> createNodeInformer(SharedInformerFactory sharedInformerFactory) {
        return sharedInformerFactory.sharedIndexInformerFor(
                Node.class,
                3000);
    }
}
