package com.netflix.titus.gateway.kubernetes;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Stopwatch;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.guice.annotation.Deactivator;
import com.netflix.titus.runtime.connector.kubernetes.KubeConnectorConfiguration;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import static com.netflix.titus.gateway.kubernetes.F8KubeObjectFormatter.formatPodEssentials;

@Singleton
public class KubeApiConnector {
    private static final Logger logger = LoggerFactory.getLogger(KubeApiConnector.class);
    private final NamespacedKubernetesClient kubernetesClient;
    private final TitusRuntime titusRuntime;
    private final KubeConnectorConfiguration configuration;
    private final ConcurrentMap<String, Pod> pods = new ConcurrentHashMap<>();

    private volatile SharedInformerFactory sharedInformerFactory;
    private volatile SharedIndexInformer<Pod> podInformer;
    private volatile SharedIndexInformer<Node> nodeInformer;

    @Inject
    public KubeApiConnector(NamespacedKubernetesClient kubernetesClient, TitusRuntime titusRuntime, KubeConnectorConfiguration configuration) {
        this.kubernetesClient = kubernetesClient;
        this.titusRuntime = titusRuntime;
        this.configuration = configuration;
    }

    private final Object activationLock = new Object();
    private volatile boolean deactivated;


    public Map<String, Pod> getPods() {
        return new HashMap<>(pods);
    }

    private SharedIndexInformer<Pod> createPodInformer(SharedInformerFactory sharedInformerFactory){
        return sharedInformerFactory.sharedIndexInformerFor(
                Pod.class,
                configuration.getKubeApiServerIntegratorRefreshIntervalMs());
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
                podInformer.addEventHandler(
                        new ResourceEventHandler<Pod>() {
                            @Override
                            public void onAdd(Pod pod) {
                                logger.info("{} pod added", pod.getMetadata().getName());
                            }

                            @Override
                            public void onUpdate(Pod oldPod, Pod newPod) {
                                logger.info("{} pod updated", oldPod.getMetadata().getName());
                            }

                            @Override
                            public void onDelete(Pod pod, boolean deletedFinalStateUnknown) {
                                logger.info("{} pod deleted", pod.getMetadata().getName());
                            }
                        }
                );
                sharedInformerFactory.startAllRegisteredInformers();
                logger.info("Kube pod informer activated");
            } catch(Exception e) {
                logger.error("Could not intialize kubernetes shared informer", e);
                if(sharedInformerFactory != null) {
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

}
