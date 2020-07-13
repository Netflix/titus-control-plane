/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Injector;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.histogram.BucketCounter;
import com.netflix.spectator.api.histogram.BucketFunctions;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.LeaseRescindedEvent;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.TaskAssignments;
import com.netflix.titus.master.mesos.TaskInfoRequest;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.V3ContainerEvent;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import com.netflix.titus.master.mesos.kubeapiserver.client.KubeApiFacade;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodStatus;
import io.kubernetes.client.openapi.models.V1ResourceRequirements;
import org.apache.mesos.Protos;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.concurrent.Queues;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import static com.netflix.titus.api.jobmanager.model.job.TaskState.Accepted;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Launched;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.StartInitiated;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Started;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;

/**
 * Responsible for integrating Kubernetes API Server concepts into Titus's Mesos based approaches.
 */
@Singleton
public class KubeApiServerIntegrator implements VirtualMachineMasterService {
    private static final Logger logger = LoggerFactory.getLogger(KubeApiServerIntegrator.class);

    private static final String ATTRIBUTE_PREFIX = "com.netflix.titus.agent.attribute/";
    private static final String KUBERNETES_NAMESPACE = "default";

    public static final String CLIENT_METRICS_PREFIX = "titusMaster.mesos.kubeApiServerIntegration";

    private static final String NEVER_RESTART_POLICY = "Never";
    private static final Quantity DEFAULT_QUANTITY = Quantity.fromString("0");

    private static final String NOT_FOUND = "Not Found";

    private static final String PENDING = "Pending";
    private static final String RUNNING = "Running";
    private static final String SUCCEEDED = "Succeeded";
    private static final String FAILED = "Failed";
    private static final String READY = "Ready";
    private static final String STOPPED = "Stopped";

    private static final String TASK_STARTING = "TASK_STARTING";

    static final String GC_UNKNOWN_PODS = "gcUnknownPods";

    static final String NODE_ATTRIBUTE_ID = "id";
    static final String NODE_ATTRIBUTE_HOST_IP = "hostIp";
    static final String NODE_ATTRIBUTE_RES = "res";
    static final String NODE_ATTRIBUTE_REGION = "region";
    static final String NODE_ATTRIBUTE_ZONE = "zone";
    static final String NODE_ATTRIBUTE_I_TYPE = "itype";
    private static final Set<String> REQUIRED_NODE_ATTRIBUTES = CollectionsExt.asSet(
            NODE_ATTRIBUTE_ID,
            NODE_ATTRIBUTE_HOST_IP,
            NODE_ATTRIBUTE_RES,
            NODE_ATTRIBUTE_REGION,
            NODE_ATTRIBUTE_ZONE,
            NODE_ATTRIBUTE_I_TYPE
    );

    private final TitusRuntime titusRuntime;
    private final MesosConfiguration mesosConfiguration;
    private final DirectKubeConfiguration directKubeConfiguration;
    private final FixedIntervalTokenBucketConfiguration gcUnknownPodsTokenBucketConfiguration;
    private final LocalScheduler scheduler;
    private final Clock clock;
    private final Injector injector;
    private final KubeApiFacade kubeApiFacade;
    private final ContainerResultCodeResolver containerResultCodeResolver;

    private final Counter launchTaskCounter;
    private final Timer launchTasksTimer;
    private final BucketCounter podSizeMetrics;
    private final Counter rejectLeaseCounter;
    private final Counter killTaskCounter;
    private final Counter nodeAddCounter;
    private final Counter nodeUpdateCounter;
    private final Counter nodeDeleteCounter;
    private final Counter podAddCounter;
    private final Counter podUpdateCounter;
    private final Counter podDeleteCounter;
    private final Gauge timedOutNodesToGcGauge;
    private final Gauge orphanedPodsWithoutValidNodesToGcGauge;
    private final Gauge terminalPodsToGcGauge;
    private final Gauge potentialUnknownPodsToGcGauge;
    private final Gauge unknownPodsToGcGauge;
    private final Gauge podsPastDeletionTimestampToGcGauge;
    private final Gauge pendingPodsWithDeletionTimestampToGcGauge;

    private com.netflix.fenzo.functions.Action1<List<? extends VirtualMachineLease>> leaseHandler;
    private Action1<List<LeaseRescindedEvent>> rescindLeaseHandler;
    private Subject<ContainerEvent, ContainerEvent> vmTaskStatusObserver;
    private V3JobOperations v3JobOperations;
    private TokenBucket gcUnknownPodsTokenBucket;

    @Inject
    public KubeApiServerIntegrator(TitusRuntime titusRuntime,
                                   MesosConfiguration mesosConfiguration,
                                   DirectKubeConfiguration directKubeConfiguration,
                                   @Named(GC_UNKNOWN_PODS) FixedIntervalTokenBucketConfiguration gcUnknownPodsTokenBucketConfiguration,
                                   LocalScheduler scheduler,
                                   Injector injector,
                                   KubeApiFacade kubeApiFacade,
                                   ContainerResultCodeResolver containerResultCodeResolver) {
        this.titusRuntime = titusRuntime;
        this.mesosConfiguration = mesosConfiguration;
        this.directKubeConfiguration = directKubeConfiguration;
        this.gcUnknownPodsTokenBucketConfiguration = gcUnknownPodsTokenBucketConfiguration;
        this.scheduler = scheduler;
        this.clock = titusRuntime.getClock();
        this.injector = injector;
        this.kubeApiFacade = kubeApiFacade;
        this.containerResultCodeResolver = containerResultCodeResolver;

        this.vmTaskStatusObserver = PublishSubject.create();

        Registry registry = titusRuntime.getRegistry();
        launchTaskCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "launchTask");
        launchTasksTimer = registry.timer(MetricConstants.METRIC_KUBERNETES + "launchTasksLatency");
        this.podSizeMetrics = BucketCounter.get(
                registry,
                registry.createId(MetricConstants.METRIC_KUBERNETES + "podSize"),
                BucketFunctions.bytes(32768)
        );
        rejectLeaseCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "rejectLease");
        killTaskCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "killTask");
        nodeAddCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "nodeAdd");
        nodeUpdateCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "nodeUpdate");
        nodeDeleteCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "nodeDelete");
        podAddCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "podAdd");
        podUpdateCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "podUpdate");
        podDeleteCounter = registry.counter(MetricConstants.METRIC_KUBERNETES + "podDelete");
        timedOutNodesToGcGauge = registry.gauge(MetricConstants.METRIC_KUBERNETES + "timedOutNodesToGc");
        orphanedPodsWithoutValidNodesToGcGauge = registry.gauge(MetricConstants.METRIC_KUBERNETES + "orphanedPodsWithoutValidNodesToGc");
        terminalPodsToGcGauge = registry.gauge(MetricConstants.METRIC_KUBERNETES + "terminalPodsToGc");
        potentialUnknownPodsToGcGauge = registry.gauge(MetricConstants.METRIC_KUBERNETES + "potentialUnknownPodsToGc");
        unknownPodsToGcGauge = registry.gauge(MetricConstants.METRIC_KUBERNETES + "unknownPodsToGc");
        podsPastDeletionTimestampToGcGauge = registry.gauge(MetricConstants.METRIC_KUBERNETES + "podsPastDeletionTimestampToGc");
        pendingPodsWithDeletionTimestampToGcGauge = registry.gauge(MetricConstants.METRIC_KUBERNETES + "pendingPodsWithDeletionTimestampToGc");
    }

    @Override
    public void enterActiveMode() {
        v3JobOperations = injector.getInstance(V3JobOperations.class);

        subscribeToNodeInformer();
        subscribeToPodInformer();

        gcUnknownPodsTokenBucket = Limiters.createInstrumentedFixedIntervalTokenBucket(
                "gcUnknownPodsTokenBucket",
                gcUnknownPodsTokenBucketConfiguration,
                currentTokenBucket -> logger.info("Detected GC unknown pods token bucket configuration update: {}", currentTokenBucket),
                titusRuntime
        );

        ScheduleDescriptor reconcileSchedulerDescriptor = ScheduleDescriptor.newBuilder()
                .withName("reconcileNodesAndPods")
                .withDescription("Reconcile nodes and pods")
                .withInitialDelay(Duration.ofSeconds(10))
                .withInterval(Duration.ofSeconds(30))
                .withTimeout(Duration.ofMinutes(5))
                .build();
        scheduler.schedule(reconcileSchedulerDescriptor, e -> reconcileNodesAndPods(), ExecutorsExt.namedSingleThreadExecutor("kube-api-server-integrator-gc"));
    }

    @Override
    public void launchTasks(TaskAssignments assignments) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if (directKubeConfiguration.isAsyncApiEnabled()) {
            launchTasksConcurrently(assignments);
            logger.info("Async pod launches completed: pods={}, elapsed={}[ms]", assignments.getCount(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        } else {
            launchTasksSync(assignments);
            logger.info("Sync pod launches completed: pods={}, elapsed={}[ms]", assignments.getCount(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
        launchTasksTimer.record(stopwatch.elapsed(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    private void launchTasksSync(TaskAssignments assignments) {
        for (TaskInfoRequest request : assignments) {
            try {
                launchTaskCounter.increment();
                V1Pod v1Pod = taskInfoToPod(request);
                logger.info("creating pod: {}", v1Pod);
                kubeApiFacade.getCoreV1Api().createNamespacedPod(KUBERNETES_NAMESPACE, v1Pod, null, null, null);
                podSizeMetrics.record(KubeUtil.estimatePodSize(v1Pod));
            } catch (Exception e) {
                logger.error("Unable to create pod with error: {}", KubeUtil.toErrorDetails(e), e);
            }
        }
    }

    private void launchTasksConcurrently(TaskAssignments assignments) {
        List<Mono<Void>> podAddActions = new ArrayList<>(assignments.getCount());
        for (TaskInfoRequest request : assignments) {
            V1Pod v1Pod = taskInfoToPod(request);
            Mono<Void> podAddAction = KubeUtil
                    .<V1Pod>toReact(handler -> kubeApiFacade.getCoreV1Api().createNamespacedPodAsync(
                            KUBERNETES_NAMESPACE, v1Pod, null, null, null, handler
                    ))
                    .doOnSubscribe(subscription -> {
                        launchTaskCounter.increment();
                        logger.info("creating pod: {}", v1Pod);
                        podSizeMetrics.record(KubeUtil.estimatePodSize(v1Pod));
                    })
                    .timeout(Duration.ofMillis(directKubeConfiguration.getKubeApiClientTimeoutMs()))
                    .ignoreElement()
                    .cast(Void.class)
                    .onErrorResume(error -> {
                        logger.error("Unable to create pod with error: {}", KubeUtil.toErrorDetails(error), error);
                        return Mono.empty();
                    });
            podAddActions.add(podAddAction);
        }

        try {
            Flux.mergeSequentialDelayError(Flux.fromIterable(podAddActions),
                    directKubeConfiguration.getPodCreateConcurrencyLimit(),
                    Queues.XS_BUFFER_SIZE
            ).blockLast();
        } catch (Exception e) {
            logger.error("Async pod create error: {}", KubeUtil.toErrorDetails(e), e);
        }
    }

    @Override
    public void rejectLease(VirtualMachineLease lease) {
        rejectLeaseCounter.increment();
    }

    @Override
    public void killTask(String taskId) {
        killTaskCounter.increment();
        try {
            logger.info("deleting pod: {}", taskId);
            kubeApiFacade.getCoreV1Api().deleteNamespacedPod(
                    taskId,
                    KUBERNETES_NAMESPACE,
                    null,
                    null,
                    directKubeConfiguration.getDeleteGracePeriodSeconds(),
                    null,
                    null,
                    null
            );
        } catch (JsonSyntaxException e) {
            // this is probably successful. the generated client has the wrong response type
        } catch (ApiException e) {
            if (e.getMessage().equalsIgnoreCase(NOT_FOUND) && taskKilledInAccepted(taskId)) {
                publishContainerEvent(taskId, Finished, REASON_TASK_KILLED, "", Optional.empty());
            } else {
                logger.error("Failed to kill task: {} with error: {}", taskId, KubeUtil.toErrorDetails(e), e);
            }
        } catch (Exception e) {
            logger.error("Failed to kill task: {} with error: {}", taskId, KubeUtil.toErrorDetails(e), e);
        }
    }

    @Override
    public void setVMLeaseHandler(Action1<List<? extends VirtualMachineLease>> leaseHandler) {
        this.leaseHandler = leaseHandler;
    }

    @Override
    public void setRescindLeaseHandler(Action1<List<LeaseRescindedEvent>> rescindLeaseHandler) {
        this.rescindLeaseHandler = rescindLeaseHandler;
    }

    @Override
    public Observable<LeaseRescindedEvent> getLeaseRescindedObservable() {
        return PublishSubject.create();
    }

    @Override
    public Observable<ContainerEvent> getTaskStatusObservable() {
        return vmTaskStatusObserver.asObservable();
    }

    private void subscribeToNodeInformer() {
        kubeApiFacade.getNodeInformer().addEventHandler(
                new ResourceEventHandler<V1Node>() {
                    @Override
                    public void onAdd(V1Node node) {
                        logger.debug("Node Added: {}", node);
                        nodeAddCounter.increment();
                        nodeUpdated(node);
                    }

                    @Override
                    public void onUpdate(V1Node oldNode, V1Node newNode) {
                        logger.debug("Node Updated Old: {}, New: {}", oldNode, newNode);
                        nodeUpdateCounter.increment();
                        nodeUpdated(newNode);
                    }

                    @Override
                    public void onDelete(V1Node node, boolean deletedFinalStateUnknown) {
                        logger.debug("Node Deleted: {}, deletedFinalStateUnknown={}", node, deletedFinalStateUnknown);
                        nodeDeleteCounter.increment();
                        nodeDeleted(node);
                    }
                });
    }

    private void nodeUpdated(V1Node node) {
        try {
            boolean notOwnedByFenzo = !KubeUtil.isNodeOwnedByFenzo(directKubeConfiguration.getFarzones(), node);
            if (notOwnedByFenzo) {
                String nodeName = node.getMetadata().getName();
                logger.debug("Ignoring node: {} as it is not owned by fenzo", nodeName);
            } else {
                VirtualMachineLease lease = nodeToLease(node);
                if (lease != null) {
                    logger.debug("Adding lease: {}", lease.getId());
                    leaseHandler.call(Collections.singletonList(lease));
                }
            }
        } catch (Exception e) {
            logger.warn("Exception on node update: {}", node, e);
        }
    }

    private void nodeDeleted(V1Node node) {
        try {
            String leaseId = node.getMetadata().getName();
            logger.debug("Removing lease on node delete: {}", leaseId);
            rescindLeaseHandler.call(Collections.singletonList(LeaseRescindedEvent.leaseIdEvent(leaseId)));
        } catch (Exception e) {
            logger.warn("Exception on node delete: {}", node, e);
        }
    }

    private void subscribeToPodInformer() {
        kubeApiFacade.getPodInformer().addEventHandler(
                new ResourceEventHandler<V1Pod>() {
                    @Override
                    public void onAdd(V1Pod pod) {
                        logger.debug("Pod Added: {}", pod);
                        podAddCounter.increment();
                        podUpdated(pod);
                    }

                    @Override
                    public void onUpdate(V1Pod oldPod, V1Pod newPod) {
                        logger.debug("Pod Updated Old: {}, New: {}", oldPod, newPod);
                        podUpdateCounter.increment();
                        podUpdated(newPod);
                    }

                    @Override
                    public void onDelete(V1Pod pod, boolean deletedFinalStateUnknown) {
                        logger.debug("Pod Deleted: {}, deletedFinalStateUnknown={}", pod, deletedFinalStateUnknown);
                        podDeleteCounter.increment();
                        podUpdated(pod);
                    }
                });
    }

    private VirtualMachineLease nodeToLease(V1Node node) {
        Protos.Offer offer = nodeToOffer(node);
        if (offer == null) {
            return null;
        }
        return new VMLeaseObject(offer);
    }

    private Protos.Offer nodeToOffer(V1Node node) {
        try {
            V1ObjectMeta metadata = node.getMetadata();
            V1NodeStatus status = node.getStatus();
            String nodeName = metadata.getName();
            boolean hasTrueReadyCondition = status.getConditions().stream()
                    .anyMatch(c -> c.getType().equalsIgnoreCase(READY) && Boolean.parseBoolean(c.getStatus()));
            if (hasTrueReadyCondition) {
                List<Protos.Attribute> nodeAttributes = nodeToAttributes(node);
                if (hasRequiredNodeAttributes(nodeAttributes)) {
                    return Protos.Offer.newBuilder()
                            .setId(Protos.OfferID.newBuilder().setValue(nodeName).build())
                            .setSlaveId(Protos.SlaveID.newBuilder().setValue(nodeName).build())
                            .setHostname(nodeName)
                            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("TitusFramework").build())
                            .addAllResources(nodeToResources(node))
                            .addAllAttributes(nodeAttributes)
                            .build();
                } else {
                    logger.debug("Ignoring node {}, as not all required attributes are set: nodeAttributes={}",
                            node.getMetadata().getName(), nodeAttributes);
                }
            }
        } catch (Exception ignore) {
            logger.info("Failed to convert node to offer for node {}", node, ignore);
        }
        return null;
    }

    private Iterable<? extends Protos.Resource> nodeToResources(V1Node node) {
        V1NodeStatus status = node.getStatus();
        Map<String, Quantity> allocatableResources = status.getAllocatable();
        List<Protos.Resource> resources = new ArrayList<>();
        resources.add(createResource("cpus", allocatableResources.getOrDefault("cpu", DEFAULT_QUANTITY).getNumber().doubleValue()));
        resources.add(createResource("mem", allocatableResources.getOrDefault("memory", DEFAULT_QUANTITY).getNumber().doubleValue()));
        resources.add(createResource("disk", allocatableResources.getOrDefault("storage", DEFAULT_QUANTITY).getNumber().doubleValue()));
        resources.add(createResource("network", allocatableResources.getOrDefault("network", DEFAULT_QUANTITY).getNumber().doubleValue()));
        resources.add(createResource("gpu", allocatableResources.getOrDefault("gpu", DEFAULT_QUANTITY).getNumber().doubleValue()));
        return resources;
    }

    private Protos.Resource createResource(String name, double value) {
        return Protos.Resource.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setName(name)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value).build())
                .build();
    }

    private List<Protos.Attribute> nodeToAttributes(V1Node node) {
        V1ObjectMeta metadata = node.getMetadata();

        List<Protos.Attribute> attributes = new ArrayList<>();
        KubeUtil.getNodeIpV4Address(node).ifPresent(nodeIp -> attributes.add(createAttribute(NODE_ATTRIBUTE_HOST_IP, nodeIp)));

        for (Map.Entry<String, String> entry : metadata.getAnnotations().entrySet()) {
            attributes.add(createAttribute(entry.getKey(), entry.getValue()));

        }
        return attributes;
    }

    @VisibleForTesting
    static boolean hasRequiredNodeAttributes(List<Protos.Attribute> nodeAttributes) {
        Set<String> names = nodeAttributes.stream().map(Protos.Attribute::getName).collect(Collectors.toSet());
        return names.containsAll(REQUIRED_NODE_ATTRIBUTES);
    }

    private Protos.Attribute createAttribute(String name, String value) {
        if (name.startsWith(ATTRIBUTE_PREFIX)) {
            name = name.replace(ATTRIBUTE_PREFIX, "");
        }
        return Protos.Attribute.newBuilder()
                .setType(Protos.Value.Type.TEXT)
                .setName(name)
                .setText(Protos.Value.Text.newBuilder().setValue(value).build())
                .build();
    }

    private V1Pod taskInfoToPod(TaskInfoRequest taskInfoRequest) {
        Protos.TaskInfo taskInfo = taskInfoRequest.getTaskInfo();
        String taskId = taskInfo.getName();
        String nodeName = taskInfo.getSlaveId().getValue();
        Map<String, String> annotations = KubeUtil.createPodAnnotations(taskInfoRequest.getJob(), taskInfoRequest.getTask(),
                taskInfo.getData().toByteArray(), taskInfoRequest.getPassthroughAttributes(),
                mesosConfiguration.isJobDescriptorAnnotationEnabled());

        V1ObjectMeta metadata = new V1ObjectMeta()
                .name(taskId)
                .annotations(annotations);

        V1Container container = new V1Container()
                .name(taskId)
                .image("imageIsInContainerInfo")
                .resources(taskInfoToResources(taskInfo));

        V1PodSpec spec = new V1PodSpec()
                .nodeName(nodeName)
                .containers(Collections.singletonList(container))
                .terminationGracePeriodSeconds(directKubeConfiguration.getPodTerminationGracePeriodSeconds())
                .restartPolicy(NEVER_RESTART_POLICY);

        return new V1Pod()
                .metadata(metadata)
                .spec(spec);
    }

    private V1ResourceRequirements taskInfoToResources(Protos.TaskInfo taskInfo) {
        Map<String, Quantity> requests = new HashMap<>();
        Map<String, Quantity> limits = new HashMap<>();
        for (Protos.Resource resource : taskInfo.getResourcesList()) {
            switch (resource.getName()) {
                case "cpus": {
                    String value = String.valueOf(resource.getScalar().getValue());
                    requests.put("cpu", new Quantity(value));
                    limits.put("cpu", new Quantity(value));
                    break;
                }
                case "mem": {
                    String value = String.valueOf(resource.getScalar().getValue());
                    requests.put("memory", new Quantity(value));
                    limits.put("memory", new Quantity(value));
                    break;
                }
                case "disk": {
                    String value = String.valueOf(resource.getScalar().getValue());
                    requests.put("titus/disk", new Quantity(value));
                    limits.put("titus/disk", new Quantity(value));
                    break;
                }
                case "network": {
                    String value = String.valueOf(resource.getScalar().getValue());
                    requests.put("titus/network", new Quantity(value));
                    limits.put("titus/network", new Quantity(value));
                    break;
                }
                case "gpu": {
                    String value = String.valueOf(resource.getScalar().getValue());
                    requests.put("titus/gpu", new Quantity(value));
                    limits.put("titus/gpu", new Quantity(value));
                    break;
                }
            }
        }
        return new V1ResourceRequirements()
                .requests(requests)
                .limits(limits);
    }

    private void podUpdated(V1Pod pod) {
        try {
            V1ObjectMeta metadata = pod.getMetadata();
            String podName = metadata.getName();

            Optional<Pair<Job<?>, Task>> taskJobOpt = v3JobOperations.findTaskById(podName);
            if (!taskJobOpt.isPresent()) {
                logger.debug("Ignoring pod update with name: {} as the task does not exist in job management", podName);
                return;
            }

            V1PodStatus status = pod.getStatus();
            String phase = status.getPhase();
            String reason = status.getReason();
            String reasonMessage = status.getMessage();
            boolean hasDeletionTimestamp = metadata.getDeletionTimestamp() != null;

            Optional<TitusExecutorDetails> executorDetails = KubeUtil.getTitusExecutorDetails(pod);
            Task task = taskJobOpt.get().getRight();

            TaskState newState;
            String reasonCode = REASON_NORMAL;

            if (phase.equalsIgnoreCase(PENDING)) {
                // inspect pod status reason to differentiate between Launched and StartInitiated (this is not standard Kubernetes)
                if (reason != null && reason.equalsIgnoreCase(TASK_STARTING)) {
                    newState = StartInitiated;
                } else {
                    newState = Launched;
                }
            } else if (phase.equalsIgnoreCase(RUNNING) && !hasDeletionTimestamp) {
                newState = Started;
            } else if (phase.equalsIgnoreCase(SUCCEEDED)) {
                newState = Finished;
                reasonCode = hasDeletionTimestamp ? REASON_TASK_KILLED : REASON_NORMAL;
            } else if (phase.equalsIgnoreCase(FAILED)) {
                newState = Finished;
                reasonCode = hasDeletionTimestamp ? REASON_TASK_KILLED : REASON_FAILED;
            } else {
                titusRuntime.getCodeInvariants().inconsistent("Pod: %s has unknown phase mapping: %s", podName, phase);
                return;
            }

            fillInMissingTaskStatusesIfNeeded(newState, pod, task, executorDetails);
            publishContainerEvent(podName, newState, reasonCode, reasonMessage, executorDetails);
        } catch (Exception e) {
            logger.error("Unable to handle pod update: {} with error:", pod, e);
        }
    }

    /**
     * If the new state is Finished and the pod had a container that ran then make sure that the StartInitiated
     * and Started states were populated in the task state machine due to apiserver sending the latest event.
     */
    private void fillInMissingTaskStatusesIfNeeded(TaskState state, V1Pod pod, Task task,
                                                   Optional<TitusExecutorDetails> executorDetailsOpt) {
        if (state != Finished) {
            return;
        }

        Optional<V1ContainerStateTerminated> terminatedContainerStatusOpt = KubeUtil.findTerminatedContainerStatus(pod);
        if (!terminatedContainerStatusOpt.isPresent()) {
            return;
        }

        String taskId = task.getId();
        V1ContainerStateTerminated containerStateTerminated = terminatedContainerStatusOpt.get();
        DateTime startedAt = containerStateTerminated.getStartedAt();
        if (startedAt != null) {
            Optional<Long> timestampOpt = Optional.of(startedAt.getMillis());
            Optional<TaskStatus> startInitiatedOpt = JobFunctions.findTaskStatus(task, StartInitiated);
            if (!startInitiatedOpt.isPresent()) {
                logger.debug("Publishing missing task status: StartInitiated for task: {}", taskId);
                publishContainerEvent(taskId, StartInitiated, TaskStatus.REASON_NORMAL, "",
                        executorDetailsOpt, timestampOpt);
            }
            Optional<TaskStatus> startedOpt = JobFunctions.findTaskStatus(task, Started);
            if (!startedOpt.isPresent()) {
                logger.debug("Publishing missing task status: Started for task: {}", taskId);
                publishContainerEvent(taskId, Started, TaskStatus.REASON_NORMAL, "",
                        executorDetailsOpt, timestampOpt);
            }
        }
    }

    private void publishContainerEvent(String taskId, TaskState taskState, String reasonCode, String reasonMessage,
                                       Optional<TitusExecutorDetails> executorDetails) {
        publishContainerEvent(taskId, taskState, reasonCode, reasonMessage, executorDetails, Optional.empty());
    }

    private void publishContainerEvent(String taskId, TaskState taskState, String reasonCode, String reasonMessage,
                                       Optional<TitusExecutorDetails> executorDetails, Optional<Long> timestampOpt) {
        V3ContainerEvent event = new V3ContainerEvent(
                taskId,
                taskState,
                containerResultCodeResolver.resolve(taskState, reasonMessage).orElse(reasonCode),
                reasonMessage,
                timestampOpt.orElseGet(() -> titusRuntime.getClock().wallTime()),
                executorDetails
        );

        logger.debug("Publishing task status: {}", event);
        vmTaskStatusObserver.onNext(event);
    }

    private void reconcileNodesAndPods() {
        if (!mesosConfiguration.isReconcilerEnabled() || !kubeApiFacade.getNodeInformer().hasSynced() || !kubeApiFacade.getPodInformer().hasSynced()) {
            return;
        }

        List<V1Node> nodes = kubeApiFacade.getNodeInformer().getIndexer().list();
        List<V1Pod> pods = kubeApiFacade.getPodInformer().getIndexer().list();
        List<Task> tasks = v3JobOperations.getTasks();
        Map<String, Task> currentTasks = tasks.stream().collect(Collectors.toMap(Task::getId, Function.identity()));

        gcTimedOutNodes(nodes);
        gcOrphanedPodsWithoutValidNodes(nodes, pods);
        gcTerminalPods(pods, currentTasks);
        gcUnknownPods(pods, currentTasks);
        gcPodsPastDeletionTimestamp(pods);
        gcPendingPodsWithDeletionTimestamp(pods);
    }

    private void gcNode(V1Node node) {
        String nodeName = node.getMetadata().getName();
        try {
            kubeApiFacade.getCoreV1Api().deleteNode(
                    nodeName,
                    null,
                    null,
                    0,
                    null,
                    "Background",
                    null
            );
        } catch (JsonSyntaxException e) {
            // this is probably successful. the generated client has the wrong response type
        } catch (ApiException e) {
            if (!e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
                logger.error("Failed to delete node: {} with error: ", nodeName, e);
            }
        } catch (Exception e) {
            logger.error("Failed to delete node: {} with error: ", nodeName, e);
        }
    }

    private boolean isNodeReadyForGc(V1Node node) {
        Optional<V1NodeCondition> stoppedConditionOpt = node.getStatus().getConditions().stream()
                .filter(c -> c.getType().equalsIgnoreCase(STOPPED) && Boolean.parseBoolean(c.getStatus()))
                .findAny();
        if (stoppedConditionOpt.isPresent()) {
            return true;
        }

        Optional<V1NodeCondition> readyConditionOpt = node.getStatus().getConditions().stream()
                .filter(c -> c.getType().equalsIgnoreCase(READY))
                .findAny();
        if (!readyConditionOpt.isPresent()) {
            return false;
        }
        V1NodeCondition readyCondition = readyConditionOpt.get();
        boolean status = Boolean.parseBoolean(readyCondition.getStatus());
        DateTime lastHeartbeatTime = readyCondition.getLastHeartbeatTime();
        return !status &&
                lastHeartbeatTime != null &&
                clock.isPast(lastHeartbeatTime.getMillis() + directKubeConfiguration.getNodeGcTtlMs());
    }

    private void gcPod(V1Pod pod) {
        String podName = pod.getMetadata().getName();
        try {
            kubeApiFacade.getCoreV1Api().deleteNamespacedPod(
                    podName,
                    KUBERNETES_NAMESPACE,
                    null,
                    null,
                    0,
                    null,
                    "Background",
                    null
            );
        } catch (JsonSyntaxException e) {
            // this is probably successful. the generated client has the wrong response type
        } catch (ApiException e) {
            if (!e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
                logger.error("Failed to delete pod: {} with error: ", podName, e);
            }
        } catch (Exception e) {
            logger.error("Failed to delete pod: {} with error: ", podName, e);
        }
    }

    /**
     * GC nodes that have timed out due to not publishing a heartbeat
     */
    private void gcTimedOutNodes(List<V1Node> nodes) {
        List<V1Node> timedOutNodesToGc = nodes.stream()
                .filter(this::isNodeReadyForGc)
                .collect(Collectors.toList());

        logger.info("Attempting to GC {} timed out nodes: {}", timedOutNodesToGc.size(), timedOutNodesToGc);
        timedOutNodesToGcGauge.set(timedOutNodesToGc.size());
        for (V1Node node : timedOutNodesToGc) {
            gcNode(node);
        }
        logger.info("Finished timed out node GC");
    }

    /**
     * GC orphaned pods on nodes that are no longer valid/available.
     */
    private void gcOrphanedPodsWithoutValidNodes(List<V1Node> nodes, List<V1Pod> pods) {
        Set<String> currentNodeNames = nodes.stream().map(n -> n.getMetadata().getName()).collect(Collectors.toSet());
        List<V1Pod> orphanedPodsWithoutValidNodesToGc = pods.stream()
                .filter(p -> {
                    String nodeName = p.getSpec().getNodeName();
                    return StringExt.isNotEmpty(nodeName) && !currentNodeNames.contains(nodeName);
                })
                .collect(Collectors.toList());

        logger.info("Attempting to GC {} orphaned pods: {} without valid nodes", orphanedPodsWithoutValidNodesToGc.size(),
                orphanedPodsWithoutValidNodesToGc);
        orphanedPodsWithoutValidNodesToGcGauge.set(orphanedPodsWithoutValidNodesToGc.size());
        for (V1Pod pod : orphanedPodsWithoutValidNodesToGc) {
            gcPod(pod);
        }
        logger.info("Finished orphaned pod GC without valid nodes");
    }

    /**
     * GC pods that have a task in a terminal state in job management or not in job management with a terminal pod phase.
     */
    private void gcTerminalPods(List<V1Pod> pods, Map<String, Task> currentTasks) {
        long now = clock.wallTime();

        List<V1Pod> terminalPodsToGc = pods.stream()
                .filter(p -> {
                    Task task = currentTasks.get(p.getMetadata().getName());
                    if (task != null) {
                        if (TaskState.isTerminalState(task.getStatus().getState())) {
                            return task.getStatus().getTimestamp() + directKubeConfiguration.getTerminatedPodGcDelayMs() <= now;
                        }
                        return false;
                    }
                    if (KubeUtil.isPodPhaseTerminal(p.getStatus().getPhase())) {
                        return KubeUtil.findFinishedTimestamp(p)
                                .map(timestamp -> timestamp + directKubeConfiguration.getTerminatedPodGcDelayMs() <= now)
                                .orElse(true);
                    }
                    return false;
                })
                .collect(Collectors.toList());

        logger.info("Attempting to GC {} terminal pods: {}", terminalPodsToGc.size(), terminalPodsToGc);
        terminalPodsToGcGauge.set(terminalPodsToGc.size());
        for (V1Pod pod : terminalPodsToGc) {
            gcPod(pod);
        }
        logger.info("Finished terminal pod GC");
    }

    /**
     * GC pods that are unknown to Titus Master that are not in a terminal pod phase.
     */
    private void gcUnknownPods(List<V1Pod> pods, Map<String, Task> currentTasks) {
        List<V1Pod> potentialUnknownPodsToGc = pods.stream()
                .filter(p -> {
                    if (KubeUtil.isPodPhaseTerminal(p.getStatus().getPhase()) || currentTasks.containsKey(p.getMetadata().getName())) {
                        return false;
                    }
                    DateTime creationTimestamp = p.getMetadata().getCreationTimestamp();
                    return creationTimestamp != null &&
                            clock.isPast(creationTimestamp.getMillis() + directKubeConfiguration.getUnknownPodGcTimeoutMs());
                })
                .collect(Collectors.toList());

        potentialUnknownPodsToGcGauge.set(potentialUnknownPodsToGc.size());

        if (!mesosConfiguration.isGcUnknownPodsEnabled()) {
            logger.info("GC unknown pods is not enabled");
            unknownPodsToGcGauge.set(0);
            return;
        }

        int numberOfPodsToGc = (int) Math.min(potentialUnknownPodsToGc.size(), gcUnknownPodsTokenBucket.getNumberOfTokens());
        if (numberOfPodsToGc > 0 && gcUnknownPodsTokenBucket.tryTake(numberOfPodsToGc)) {
            List<V1Pod> unknownPodsToGc = potentialUnknownPodsToGc.subList(0, numberOfPodsToGc);
            logger.info("Attempting to GC {} unknown pods: {}", unknownPodsToGc.size(), unknownPodsToGc);
            unknownPodsToGcGauge.set(unknownPodsToGc.size());
            for (V1Pod pod : unknownPodsToGc) {
                gcPod(pod);
            }
            logger.info("Finished unknown pod GC");
        } else {
            unknownPodsToGcGauge.set(0);
        }
    }

    /**
     * GC pods past deletion timestamp timeout.
     */
    private void gcPodsPastDeletionTimestamp(List<V1Pod> pods) {
        List<V1Pod> podsPastDeletionTimestampToGc = pods.stream()
                .filter(p -> {
                    DateTime deletionTimestamp = p.getMetadata().getDeletionTimestamp();
                    return deletionTimestamp != null &&
                            clock.isPast(deletionTimestamp.getMillis() + directKubeConfiguration.getDeleteGracePeriodSeconds()
                                    + directKubeConfiguration.getPodTerminationGcTimeoutMs());
                })
                .collect(Collectors.toList());

        logger.info("Attempting to GC {} pods: {} past deletion timestamp", podsPastDeletionTimestampToGc.size(),
                podsPastDeletionTimestampToGc);
        podsPastDeletionTimestampToGcGauge.set(podsPastDeletionTimestampToGc.size());
        for (V1Pod pod : podsPastDeletionTimestampToGc) {
            gcPod(pod);
        }
        logger.info("Finished pods past deletion timestamp GC");
    }

    /**
     * GC pods in Pending phase with a deletion timestamp.
     */
    private void gcPendingPodsWithDeletionTimestamp(List<V1Pod> pods) {
        List<V1Pod> pendingPodsWithDeletionTimestampToGc = pods.stream()
                .filter(p -> {
                    DateTime deletionTimestamp = p.getMetadata().getDeletionTimestamp();
                    return p.getStatus().getPhase().equalsIgnoreCase(PENDING) && deletionTimestamp != null;
                })
                .collect(Collectors.toList());

        logger.info("Attempting to GC {} pending pods: {} with deletion timestamp", pendingPodsWithDeletionTimestampToGc.size(),
                pendingPodsWithDeletionTimestampToGc);
        pendingPodsWithDeletionTimestampToGcGauge.set(pendingPodsWithDeletionTimestampToGc.size());
        for (V1Pod pod : pendingPodsWithDeletionTimestampToGc) {
            gcPod(pod);
            publishContainerEvent(pod.getMetadata().getName(), Finished, REASON_TASK_KILLED, "", Optional.empty());
        }
        logger.info("Finished pending pods with deletion timestamp GC");
    }

    private boolean taskKilledInAccepted(String taskId) {
        Optional<Pair<Job<?>, Task>> taskJobPair = v3JobOperations.findTaskById(taskId);
        if (!taskJobPair.isPresent()) {
            return false;
        }

        Task task = taskJobPair.get().getRight();
        return task.getStatus().getState() == Accepted;
    }
}
