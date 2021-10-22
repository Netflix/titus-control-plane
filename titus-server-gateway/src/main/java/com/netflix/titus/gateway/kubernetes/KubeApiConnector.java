package com.netflix.titus.gateway.kubernetes;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.naming.Name;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.runtime.connector.kubernetes.Fabric8IOClients;
import com.netflix.titus.runtime.connector.kubernetes.KubeConnectorConfiguration;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import static com.netflix.titus.gateway.kubernetes.F8KubeObjectFormatter.formatPodEssentials;

@Singleton
public class KubeApiConnector {
    private static final Logger logger = LoggerFactory.getLogger(KubeApiConnector.class);
    private final NamespacedKubernetesClient kubernetesClient;
    private final TitusRuntime titusRuntime;
    private final ConcurrentMap<String, Pod> pods = new ConcurrentHashMap<>();
    private volatile SharedInformerFactory sharedInformerFactory;
    private volatile SharedIndexInformer<Pod> podInformer;
    private volatile SharedIndexInformer<Node> nodeInformer;

    private ExecutorService notificationHandlerExecutor;
    private Scheduler scheduler;
    private Disposable subscription;

    @Inject
    public KubeApiConnector(NamespacedKubernetesClient kubernetesClient, TitusRuntime titusRuntime) {
        this.kubernetesClient = kubernetesClient;
        this.titusRuntime = titusRuntime;
    }

    private final Object activationLock = new Object();
    private volatile boolean deactivated;


    public Map<String, Pod> getPods() {
        return new HashMap<>(pods);
    }

    private SharedIndexInformer<Pod> createPodInformer(SharedInformerFactory sharedInformerFactory) {
        return sharedInformerFactory.sharedIndexInformerFor(
                Pod.class,
                3000);
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

    public SharedIndexInformer<Pod> getPodInformer() {
        activate();
        return podInformer;
    }

    public SharedIndexInformer<Node> getNodeInformer() {
        activate();
        return nodeInformer;
    }

    private void activate() {
        synchronized (activationLock) {
            if (sharedInformerFactory != null) {
                return;
            }
            try {
                this.sharedInformerFactory = kubernetesClient.informers();
                this.podInformer = createPodInformer(sharedInformerFactory);
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


    public Flux<PodEvent> events() {
        return kubeInformerEvents().transformDeferred(ReactorExt.badSubscriberHandler(logger));
    }

    private Flux<PodEvent> kubeInformerEvents() {
        return Flux.create(sink -> {
            ResourceEventHandler<Pod> handler = new ResourceEventHandler<Pod>() {
                @Override
                public void onAdd(Pod pod) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {

                        String taskId = pod.getSpec().getContainers().get(0).getName();

                        Pod old = pods.get(taskId);
                        pods.put(taskId, pod);

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
                        //metrics.onUpdate(newPod);

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
                public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    try {
                        //metrics.onDelete(pod);

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
            this.getPodInformer().addEventHandler(handler);

            // A listener cannot be removed from shared informer.
            // sink.onCancel(() -> ???);
        });
    }

    @Activator
    public void enterActiveMode() {
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
    private Mono<Void> processEvent(PodEvent event) {
        return Mono.empty();
    }

    @VisibleForTesting
    protected Scheduler initializeNotificationScheduler() {
        this.notificationHandlerExecutor = ExecutorsExt.namedSingleThreadExecutor(KubeApiConnector.class.getSimpleName());
        return Schedulers.fromExecutor(notificationHandlerExecutor);
    }

}