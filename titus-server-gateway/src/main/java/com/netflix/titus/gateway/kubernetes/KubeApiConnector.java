package com.netflix.titus.gateway.kubernetes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStatus;
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
    private static final Logger logger = LoggerFactory.getLogger("KubeSharedInformerLogger");
    private final NamespacedKubernetesClient kubernetesClient;
    private final TitusRuntime titusRuntime;
    private final ConcurrentMap<String, Pod> pods = new ConcurrentHashMap<>();
    private volatile SharedInformerFactory sharedInformerFactory;
    private volatile SharedIndexInformer<Pod> podInformer;
    private volatile SharedIndexInformer<Node> nodeInformer;
    private final ReadOnlyJobOperations jobService;

    private ExecutorService notificationHandlerExecutor;
    private Scheduler scheduler;
    private Disposable subscription;

    @Inject
    public KubeApiConnector(NamespacedKubernetesClient kubernetesClient, TitusRuntime titusRuntime,
                            ReadOnlyJobOperations jobService) {
        this.kubernetesClient = kubernetesClient;
        this.titusRuntime = titusRuntime;
        this.jobService = jobService;
        enterActiveMode();
    }

    private final Object activationLock = new Object();
    private volatile boolean deactivated;

    public List<TaskStatus.ContainerState> getContainerState(String taskId) {
        if(pods.get(taskId) == null) {
            return Collections.emptyList();
        }
        List<ContainerStatus> containerStatuses = pods.get(taskId).getStatus().getContainerStatuses();
        ArrayList<TaskStatus.ContainerState> containerStates = new ArrayList();
        if (containerStates == null) {
            return Collections.emptyList();
        }
        if (containerStatuses.isEmpty() || containerStatuses.size() == 0) {
            // we have pod status but no container status just yet
            return Collections.emptyList();
        } else {
            ListIterator<ContainerStatus> iterator = containerStatuses.listIterator();
            while (iterator.hasNext()) {
                ContainerStatus containerStatus = iterator.next();
                ContainerState status = containerStatus.getState();
                TaskStatus.ContainerState.ContainerHealth containerHealth = TaskStatus.ContainerState.ContainerHealth.Unset;
                if(status.toString().equals("running")) {
                    containerHealth = TaskStatus.ContainerState.ContainerHealth.Healthy;
                } else if (status.toString().equals("waiting")) {
                    containerHealth = TaskStatus.ContainerState.ContainerHealth.Unhealthy;
                }
                containerStates.add(TaskStatus.ContainerState.newBuilder().setContainerName(containerStatus.getName())
                        .setContainerHealth(containerHealth).build());
            }
            return containerStates;
        }
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

    private Mono<Void> processEvent(PodEvent event) {
        Pair<Job<?>, Task> jobAndTask = jobService.findTaskById(event.getTaskId()).orElse(null);
        if (jobAndTask == null) {
            logger.warn("Got Kube notification about unknown task: {}", event.getTaskId());
            return Mono.empty();
        }

        Task task = jobAndTask.getRight();
        // This is basic sanity check. If it fails, we have a major problem with pod state.
        if (event.getPod() == null || event.getPod().getStatus() == null || event.getPod().getStatus().getPhase() == null) {
            logger.warn("Pod notification with pod without status or phase set: taskId={}, pod={}", task.getId(), event.getPod());
            return Mono.empty();
        }

        return Mono.empty();
    }

    @VisibleForTesting
    protected Scheduler initializeNotificationScheduler() {
        this.notificationHandlerExecutor = ExecutorsExt.namedSingleThreadExecutor(KubeApiConnector.class.getSimpleName());
        return Schedulers.fromExecutor(notificationHandlerExecutor);
    }

}
