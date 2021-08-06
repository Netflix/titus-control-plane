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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.gson.JsonSyntaxException;
import com.google.inject.Injector;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.spectator.api.Counter;
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
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.kubernetes.pod.KubePodConfiguration;
import com.netflix.titus.master.kubernetes.pod.PodFactory;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.LeaseRescindedEvent;
import com.netflix.titus.master.mesos.TaskAssignments;
import com.netflix.titus.master.mesos.TaskInfoRequest;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.V3ContainerEvent;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import com.netflix.titus.master.mesos.kubeapiserver.direct.DirectKubeConfiguration;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
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
import static com.netflix.titus.api.jobmanager.model.job.TaskState.KillInitiated;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Launched;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.StartInitiated;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Started;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.isBefore;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.isTerminalState;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_CRASHED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_STUCK_IN_STATE;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_UNKNOWN;
import static com.netflix.titus.master.mesos.kubeapiserver.KubeObjectFormatter.formatNodeEssentials;
import static com.netflix.titus.master.mesos.kubeapiserver.KubeObjectFormatter.formatPodEssentials;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.DEFAULT_NAMESPACE;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.FAILED;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.NODE_LOST;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.NOT_FOUND;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.PENDING;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.RUNNING;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.SUCCEEDED;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.TITUS_NODE_DOMAIN;

/**
 * Responsible for integrating Kubernetes API Server concepts into Titus's Mesos based approaches.
 */
@Singleton
public class KubeApiServerIntegrator implements VirtualMachineMasterService {
    private static final Logger logger = LoggerFactory.getLogger(KubeApiServerIntegrator.class);

    public static final String CLIENT_METRICS_PREFIX = "titusMaster.mesos.kubeApiServerIntegration";

    private static final Quantity DEFAULT_QUANTITY = Quantity.fromString("0");

    private static final String TASK_STARTING = "TASK_STARTING";

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
    private final DirectKubeConfiguration directKubeConfiguration;
    private final KubePodConfiguration kubePodConfiguration;
    private final Injector injector;
    private final KubeApiFacade kubeApiFacade;
    private final ContainerResultCodeResolver containerResultCodeResolver;
    private final PodFactory podFactory;

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

    private final Subject<ContainerEvent, ContainerEvent> vmTaskStatusObserver;

    private com.netflix.fenzo.functions.Action1<List<? extends VirtualMachineLease>> leaseHandler;
    private Action1<List<LeaseRescindedEvent>> rescindLeaseHandler;
    private V3JobOperations v3JobOperations;

    @Inject
    public KubeApiServerIntegrator(TitusRuntime titusRuntime,
                                   DirectKubeConfiguration directKubeConfiguration,
                                   KubePodConfiguration kubePodConfiguration,
                                   Injector injector,
                                   KubeApiFacade kubeApiFacade,
                                   ContainerResultCodeResolver containerResultCodeResolver,
                                   PodFactory podFactory) {
        this.titusRuntime = titusRuntime;
        this.directKubeConfiguration = directKubeConfiguration;
        this.kubePodConfiguration = kubePodConfiguration;
        this.injector = injector;
        this.kubeApiFacade = kubeApiFacade;
        this.containerResultCodeResolver = containerResultCodeResolver;
        this.podFactory = podFactory;

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
    }

    @Override
    public void enterActiveMode() {
        v3JobOperations = injector.getInstance(V3JobOperations.class);

        subscribeToNodeInformer();
        subscribeToPodInformer();
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
                logger.info("creating pod: {}", formatPodEssentials(v1Pod));
                logger.debug("complete pod data: {}", v1Pod);
                kubeApiFacade.createNamespacedPod(DEFAULT_NAMESPACE, v1Pod);
                podSizeMetrics.record(KubeUtil.estimatePodSize(v1Pod));
            } catch (Exception e) {
                String errorMessage = KubeUtil.toErrorDetails(e);
                logger.error("Unable to create pod with error: {}", errorMessage, e);
                publishOnPodCreateErrorEvent(request.getTask(), errorMessage);
            }
        }
    }

    private void launchTasksConcurrently(TaskAssignments assignments) {
        List<Mono<Void>> podAddActions = new ArrayList<>(assignments.getCount());
        for (TaskInfoRequest request : assignments) {
            V1Pod v1Pod = taskInfoToPod(request);
            Mono<Void> podAddAction = KubeUtil
                    .<V1Pod>toReact(handler -> kubeApiFacade.createNamespacedPodAsync(DEFAULT_NAMESPACE, v1Pod, handler))
                    .doOnSubscribe(subscription -> {
                        launchTaskCounter.increment();
                        logger.info("creating pod: {}", formatPodEssentials(v1Pod));
                        logger.debug("complete pod data: {}", v1Pod);
                        podSizeMetrics.record(KubeUtil.estimatePodSize(v1Pod));
                    })
                    .timeout(Duration.ofMillis(directKubeConfiguration.getKubeApiClientTimeoutMs()))
                    .ignoreElement()
                    .cast(Void.class)
                    .onErrorResume(error -> {
                        String errorMessage = KubeUtil.toErrorDetails(error);
                        logger.error("Unable to create pod with error: {}", errorMessage, error);
                        publishOnPodCreateErrorEvent(request.getTask(), errorMessage);
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

    /**
     * Publish the Finished event for tasks (pods) that could not be created due to an error so they will get
     * replaced with a new task.
     */
    private void publishOnPodCreateErrorEvent(Task task, String errorMessage) {
        publishContainerEvent(
                task.getId(),
                Finished,
                REASON_CRASHED,
                "Failed to create pod: " + errorMessage,
                Optional.empty()
        );
    }

    @Override
    public void rejectLease(VirtualMachineLease lease) {
        rejectLeaseCounter.increment();
    }

    @Override
    public void killTask(String taskId) {
        killTaskCounter.increment();
        try {
            logger.info("Terminating pod: {} by setting deletionTimestamp", taskId);
            kubeApiFacade.deleteNamespacedPod(DEFAULT_NAMESPACE, taskId, directKubeConfiguration.getDeleteGracePeriodSeconds());
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
            boolean notOwnedByFenzo = !KubeUtil.isNodeOwnedByFenzo(kubePodConfiguration.getFarzones(), node);
            if (notOwnedByFenzo) {
                String nodeName = KubeUtil.getMetadataName(node.getMetadata());
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
            String nodeName = KubeUtil.getMetadataName(node.getMetadata());
            logger.debug("Removing lease on node delete: {}", nodeName);
            rescindLeaseHandler.call(Collections.singletonList(LeaseRescindedEvent.leaseIdEvent(nodeName)));
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
                        podDeleted(pod);
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

            if (metadata == null || status == null || status.getConditions() == null ||
                    CollectionsExt.isNullOrEmpty(status.getAllocatable()) || CollectionsExt.isNullOrEmpty(status.getCapacity())) {
                logger.debug("Ignoring node as it is missing fields: {}", node);
                return null;
            }

            String nodeName = KubeUtil.getMetadataName(metadata);
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
                        nodeName, nodeAttributes);
            }
        } catch (Exception e) {
            logger.info("Failed to convert node to offer for node: {}", formatNodeEssentials(node), e);
            logger.debug("Complete node: {}", node);
        }
        return null;
    }

    private Iterable<? extends Protos.Resource> nodeToResources(V1Node node) {
        V1NodeStatus status = node.getStatus();
        if (status == null) {
            return Collections.emptyList();
        }

        Map<String, Quantity> allocatableResources = status.getAllocatable();
        if (allocatableResources == null || allocatableResources.isEmpty()) {
            return Collections.emptyList();
        }

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
        if (metadata == null) {
            return Collections.emptyList();
        }

        Map<String, String> annotations = metadata.getAnnotations();
        if (annotations == null || annotations.isEmpty()) {
            return Collections.emptyList();
        }

        // Validate the res value is correct otherwise reject the node until it is correct.
        // TODO This code is temporary and will be changed as the node contract is changing.
        String resValue = annotations.get(TITUS_NODE_DOMAIN + NODE_ATTRIBUTE_RES);
        if (StringExt.isEmpty(resValue) || !resValue.startsWith("ResourceSet-ENIs-")) {
            return Collections.emptyList();
        }

        List<Protos.Attribute> attributes = new ArrayList<>();
        KubeUtil.getNodeIpV4Address(node).ifPresent(nodeIp -> attributes.add(createAttribute(NODE_ATTRIBUTE_HOST_IP, nodeIp)));

        for (Map.Entry<String, String> entry : annotations.entrySet()) {
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
        if (name.startsWith(TITUS_NODE_DOMAIN)) {
            name = name.replace(TITUS_NODE_DOMAIN, "");
        }
        return Protos.Attribute.newBuilder()
                .setType(Protos.Value.Type.TEXT)
                .setName(name)
                .setText(Protos.Value.Text.newBuilder().setValue(value).build())
                .build();
    }

    private V1Pod taskInfoToPod(TaskInfoRequest taskInfoRequest) {
        return podFactory.buildV1Pod(taskInfoRequest.getJob(), taskInfoRequest.getTask(), false, directKubeConfiguration.isEbsVolumePvEnabled());
    }

    private void podUpdated(V1Pod pod) {
        try {
            V1ObjectMeta metadata = pod.getMetadata();
            V1PodStatus status = pod.getStatus();
            String podName = KubeUtil.getMetadataName(metadata);

            if (StringExt.isEmpty(podName) || status == null) {
                logger.warn("Pod: {} is invalid", pod);
                return;
            }

            Optional<Task> taskOpt = getTaskIfNotTerminal(podName);
            if (!taskOpt.isPresent()) {
                return;
            }

            Task task = taskOpt.get();
            String phase = StringExt.nonNull(status.getPhase());
            String reason = StringExt.nonNull(status.getReason());
            String reasonMessage = StringExt.nonNull(status.getMessage());
            boolean hasDeletionTimestamp = metadata.getDeletionTimestamp() != null;
            Optional<TitusExecutorDetails> executorDetails = KubeUtil.getTitusExecutorDetails(pod);

            TaskState newState;
            String reasonCode = REASON_NORMAL;

            TaskState taskState = task.getStatus().getState();
            if (phase.equalsIgnoreCase(PENDING)) {
                // inspect pod status reason to differentiate between Launched and StartInitiated (this is not standard Kubernetes)
                if (reason.equalsIgnoreCase(TASK_STARTING)) {
                    newState = StartInitiated;
                } else {
                    newState = Launched;
                }
                // Check for races. Do not allow setting back task state.
                if (isBefore(newState, taskState)) {
                    titusRuntime.getCodeInvariants().inconsistent("Pod: %s in state not consistent with the task state: pod=%s, task=%s", podName, newState, taskState);
                    return;
                }
            } else if (phase.equalsIgnoreCase(RUNNING)) {
                newState = Started;
                // Check for races. Do not allow setting back task state.
                if (isBefore(newState, taskState)) {
                    titusRuntime.getCodeInvariants().inconsistent("Pod: %s in state not consistent with the task state: pod=%s, task=%s", podName, newState, taskState);
                    return;
                }
            } else if (phase.equalsIgnoreCase(SUCCEEDED)) {
                newState = Finished;
                if (taskState == KillInitiated && task.getStatus().getReasonCode().equals(REASON_STUCK_IN_STATE)) {
                    reasonCode = REASON_TRANSIENT_SYSTEM_ERROR;
                } else {
                    if (hasDeletionTimestamp || taskState == KillInitiated) {
                        reasonCode = REASON_TASK_KILLED;
                    } else {
                        reasonCode = REASON_NORMAL;
                    }
                }
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

    private void podDeleted(V1Pod pod) {
        try {
            Optional<Long> eventTimestamp = Optional.of(titusRuntime.getClock().wallTime());
            V1ObjectMeta metadata = pod.getMetadata();
            V1PodStatus status = pod.getStatus();
            String podName = KubeUtil.getMetadataName(metadata);

            if (StringExt.isEmpty(podName)) {
                logger.warn("Pod: {} is missing a name", pod);
                return;
            }

            Optional<Task> taskOpt = getTaskIfNotTerminal(podName);
            if (!taskOpt.isPresent()) {
                return;
            }

            Optional<TitusExecutorDetails> executorDetails = KubeUtil.getTitusExecutorDetails(pod);

            if (status == null) {
                publishContainerEvent(podName, Finished, REASON_UNKNOWN, "Container was terminated and did not have a pod status field", executorDetails);
                return;
            }

            Task task = taskOpt.get();
            String phase = StringExt.nonNull(status.getPhase());
            String reasonMessage = StringExt.nonNull(status.getMessage());
            String reasonCode;

            Optional<TaskStatus> killInitiatedOpt = JobFunctions.findTaskStatus(task, KillInitiated);
            if (!killInitiatedOpt.isPresent()) {
                reasonCode = REASON_TASK_KILLED;
                logger.debug("Publishing missing task status: KillInitiated for task: {}", podName);
                if (NODE_LOST.equals(status.getReason())) {
                    publishContainerEvent(podName, KillInitiated, reasonCode, "The host running the container was unexpectedly terminated", executorDetails, eventTimestamp);
                } else {
                    publishContainerEvent(podName, KillInitiated, reasonCode, "Container was terminated without going through the Titus API", executorDetails, eventTimestamp);
                }
            } else if (phase.equalsIgnoreCase(PENDING)) {
                // Pod in pending phase which is being deleted must have been killed, as it was never run.
                TaskState taskState = task.getStatus().getState();
                if (taskState == KillInitiated && task.getStatus().getReasonCode().equals(REASON_STUCK_IN_STATE)) {
                    reasonCode = REASON_TRANSIENT_SYSTEM_ERROR;
                } else {
                    reasonCode = REASON_TASK_KILLED;
                }
            } else if (phase.equalsIgnoreCase(SUCCEEDED)) {
                TaskState taskState = task.getStatus().getState();
                if (taskState == KillInitiated && task.getStatus().getReasonCode().equals(REASON_STUCK_IN_STATE)) {
                    reasonCode = REASON_TRANSIENT_SYSTEM_ERROR;
                } else {
                    boolean hasDeletionTimestamp = metadata != null && metadata.getDeletionTimestamp() != null;
                    if (hasDeletionTimestamp || taskState == KillInitiated) {
                        reasonCode = REASON_TASK_KILLED;
                    } else {
                        reasonCode = REASON_NORMAL;
                    }
                }
            } else if (phase.equalsIgnoreCase(FAILED)) {
                reasonCode = REASON_FAILED;
            } else {
                titusRuntime.getCodeInvariants().inconsistent("Pod: %s has unknown phase mapping: %s", podName, phase);
                reasonCode = REASON_UNKNOWN;
            }

            publishContainerEvent(podName, Finished, reasonCode, reasonMessage, executorDetails, eventTimestamp);
        } catch (Exception e) {
            logger.error("Unable to handle pod delete: {} with error:", pod, e);
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
                publishContainerEvent(taskId, StartInitiated, TaskStatus.REASON_NORMAL, "(Filling in missed event)",
                        executorDetailsOpt, timestampOpt);
            }
            Optional<TaskStatus> startedOpt = JobFunctions.findTaskStatus(task, Started);
            if (!startedOpt.isPresent()) {
                logger.debug("Publishing missing task status: Started for task: {}", taskId);
                publishContainerEvent(taskId, Started, TaskStatus.REASON_NORMAL, "(Filling in missed event)",
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

    private boolean taskKilledInAccepted(String taskId) {
        Optional<Pair<Job<?>, Task>> taskJobPair = v3JobOperations.findTaskById(taskId);
        if (!taskJobPair.isPresent()) {
            return false;
        }

        Task task = taskJobPair.get().getRight();
        return task.getStatus().getState() == Accepted;
    }

    private Optional<Task> getTaskIfNotTerminal(String podName) {
        Optional<Pair<Job<?>, Task>> taskJobOpt = v3JobOperations.findTaskById(podName);
        if (!taskJobOpt.isPresent()) {
            logger.debug("Pod with name: {} does not exist in job management", podName);
            return Optional.empty();
        }

        Task task = taskJobOpt.get().getRight();
        TaskState taskState = task.getStatus().getState();
        if (isTerminalState(taskState)) {
            logger.debug("Pod with name: {} is in the terminal state: {}", podName, taskState.name());
            return Optional.empty();
        }

        return Optional.of(task);
    }
}
