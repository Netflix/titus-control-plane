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
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Strings;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Injector;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.network.client.ClientMetrics;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.LeaseRescindedEvent;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.V3ContainerEvent;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeCondition;
import io.kubernetes.client.models.V1NodeList;
import io.kubernetes.client.models.V1NodeStatus;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.Watch;
import org.apache.mesos.Protos;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import static com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Launched;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.StartInitiated;
import static com.netflix.titus.api.jobmanager.model.job.TaskState.Started;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
import static com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;

/**
 * Responsible for integrating Kubernetes API Server concepts into Titus's Mesos based approaches.
 */
@Experimental(detail = "This is a basic integration with the kubernetes api server", deadline = "9/1/2019")
@Singleton
public class KubeApiServerIntegrator implements VirtualMachineMasterService {
    private static final Logger logger = LoggerFactory.getLogger(KubeApiServerIntegrator.class);

    private static final String ATTRIBUTE_PREFIX = "com.netflix.titus.agent.attribute/";
    private static final String NODE_ID_ATTRIBUTE = ATTRIBUTE_PREFIX + "id";
    private static final String KUBERNETES_NAMESPACE = "default";
    private static final String CLIENT_METRICS_PREFIX = "titusMaster.mesos.kubeApiServerIntegration";
    private static final long POD_TERMINATION_GRACE_PERIOD_SECONDS = 600L;
    private static final int DELETE_GRACE_PERIOD_SECONDS = 300;
    private static final int NODE_GC_TTL_MS = 60_000;
    private static final int ORPHANED_POD_TIMEOUT_MS = 60_000;

    private static final String POST = "POST";
    private static final String GET = "GET";
    private static final String DELETE = "DELETE";
    private static final String STATUS_200 = "200";
    private static final String NOT_FOUND = "Not found";
    private static final String NODES = "nodes";
    private static final String PODS = "pods";

    private static final String PENDING = "Pending";
    private static final String RUNNING = "Running";
    private static final String SUCCEEDED = "Succeeded";
    private static final String FAILED = "Failed";
    private static final String READY = "Ready";
    private static final String STOPPED = "Stopped";

    private static final String TASK_STARTING = "TASK_STARTING";

    private final TitusRuntime titusRuntime;
    private final MesosConfiguration mesosConfiguration;
    private final LocalScheduler scheduler;
    private final Clock clock;
    private final Injector injector;

    private final Function<String, Matcher> invalidRequestMessageMatcherFactory;
    private final Function<String, Matcher> crashedMessageMatcherFactory;
    private final Function<String, Matcher> transientSystemErrorMessageMatcherFactory;
    private final Function<String, Matcher> localSystemErrorMessageMatcherFactory;
    private final Function<String, Matcher> unknownSystemErrorMessageMatcherFactory;

    private volatile Snapshot<V1Node> lastSnapshot = Snapshot.emptySnapshot();

    private com.netflix.fenzo.functions.Action1<List<? extends VirtualMachineLease>> leaseHandler;
    private Action1<List<LeaseRescindedEvent>> rescindLeaseHandler;
    private Subject<ContainerEvent, ContainerEvent> vmTaskStatusObserver;
    private V3JobOperations v3JobOperations;
    private ClientMetrics nodesClientMetrics;
    private ClientMetrics podsClientMetrics;
    private CoreV1Api normalApi;
    private ApiClient watchClient;
    private CoreV1Api watchApi;

    @Inject
    public KubeApiServerIntegrator(TitusRuntime titusRuntime,
                                   MesosConfiguration mesosConfiguration,
                                   LocalScheduler scheduler,
                                   Injector injector) {
        this.titusRuntime = titusRuntime;
        this.mesosConfiguration = mesosConfiguration;
        this.scheduler = scheduler;
        this.clock = titusRuntime.getClock();
        this.injector = injector;

        this.vmTaskStatusObserver = PublishSubject.create();

        nodesClientMetrics = new ClientMetrics(CLIENT_METRICS_PREFIX, NODES, titusRuntime.getRegistry(), clock);
        podsClientMetrics = new ClientMetrics(CLIENT_METRICS_PREFIX, PODS, titusRuntime.getRegistry(), clock);

        ApiClient normalClient = Config.fromUrl(mesosConfiguration.getKubeApiServerUrl());
        normalClient.getHttpClient().setReadTimeout(60_000, TimeUnit.MILLISECONDS);
        normalApi = new CoreV1Api(normalClient);

        watchClient = Config.fromUrl(mesosConfiguration.getKubeApiServerUrl());
        watchClient.getHttpClient().setReadTimeout(1_800_000, TimeUnit.MILLISECONDS);
        watchApi = new CoreV1Api(watchClient);

        invalidRequestMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getInvalidRequestMessagePattern, "invalidRequestMessagePattern", Pattern.DOTALL, logger);
        crashedMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getCrashedMessagePattern, "crashedMessagePattern", Pattern.DOTALL, logger);
        transientSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getTransientSystemErrorMessagePattern, "transientSystemErrorMessagePattern", Pattern.DOTALL, logger);
        localSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getLocalSystemErrorMessagePattern, "localSystemErrorMessagePattern", Pattern.DOTALL, logger);
        unknownSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getUnknownSystemErrorMessagePattern, "unknownSystemErrorMessagePattern", Pattern.DOTALL, logger);
    }

    @Override
    public void enterActiveMode() {
        v3JobOperations = injector.getInstance(V3JobOperations.class);

        ScheduleDescriptor getNodeUpdatesSchedulerDescriptor = ScheduleDescriptor.newBuilder()
                .withName("getNodeUpdates")
                .withDescription("Get pod information")
                .withInitialDelay(Duration.ofSeconds(5))
                .withInterval(Duration.ofSeconds(5))
                .withTimeout(Duration.ofMinutes(5))
                .build();
        scheduler.schedule(getNodeUpdatesSchedulerDescriptor, e -> getNodeUpdates(), ExecutorsExt.namedSingleThreadExecutor("kube-api-server-integrator-get-node-updates"));

        ScheduleDescriptor getPodUpdatesSchedulerDescriptor = ScheduleDescriptor.newBuilder()
                .withName("getPodUpdates")
                .withDescription("Get pod information")
                .withInitialDelay(Duration.ofSeconds(5))
                .withInterval(Duration.ofSeconds(5))
                .withTimeout(Duration.ofHours(1))
                .build();
        scheduler.schedule(getPodUpdatesSchedulerDescriptor, e -> getPodUpdates(), ExecutorsExt.namedSingleThreadExecutor("kube-api-server-integrator-get-pod-updates"));

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
    public void launchTasks(List<Protos.TaskInfo> requests, List<VirtualMachineLease> leases) {
        for (Protos.TaskInfo request : requests) {
            long startTimeMs = clock.wallTime();
            try {
                V1Pod v1Pod = taskInfoToPod(request);
                logger.info("creating pod: {}", v1Pod);
                normalApi.createNamespacedPod(KUBERNETES_NAMESPACE, v1Pod, false, null, null);
                podsClientMetrics.incrementOnSuccess(POST, PODS, STATUS_200);
                podsClientMetrics.registerOnSuccessLatency(POST, Duration.ofMillis(clock.wallTime() - startTimeMs));
            } catch (ApiException e) {
                logger.error("Unable to create pod with error:", e);
                podsClientMetrics.incrementOnError(POST, PODS, e);
                podsClientMetrics.registerOnErrorLatency(POST, Duration.ofMillis(clock.wallTime() - startTimeMs));
            }
        }
    }

    private V1Pod taskInfoToPod(Protos.TaskInfo taskInfo) {
        String taskId = taskInfo.getName();
        String nodeName = taskInfo.getSlaveId().getValue();
        String encodedContainerInfo = Base64.getEncoder().encodeToString(taskInfo.getData().toByteArray());

        Map<String, String> annotations = new HashMap<>();
        annotations.put("containerInfo", encodedContainerInfo);
        annotations.putAll(PerformanceToolUtil.findPerformanceTestAnnotations(taskInfo));

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
                .terminationGracePeriodSeconds(POD_TERMINATION_GRACE_PERIOD_SECONDS);

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
            }
        }
        return new V1ResourceRequirements()
                .requests(requests)
                .limits(limits);
    }

    @Override
    public void rejectLease(VirtualMachineLease lease) {
        // do nothing
    }

    @Override
    public void killTask(String taskId) {
        long startTimeMs = clock.wallTime();
        try {
            logger.info("deleting pod: {}", taskId);
            normalApi.deleteNamespacedPod(taskId, KUBERNETES_NAMESPACE, null, null, null, DELETE_GRACE_PERIOD_SECONDS, null, null);
            podsClientMetrics.incrementOnSuccess(DELETE, PODS, STATUS_200);
            podsClientMetrics.registerOnSuccessLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (JsonSyntaxException e) {
            // this is probably successful. the generated client has the wrong response type
            podsClientMetrics.incrementOnSuccess(DELETE, PODS, STATUS_200);
            podsClientMetrics.registerOnSuccessLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (ApiException e) {
            if (e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
                // move task to terminal state if it is not found in the api server
                publishContainerEvent(taskId, Finished, REASON_TASK_KILLED, "", Optional.empty());
            } else {
                logger.error("Failed to kill task: {} with error: ", taskId, e);
                podsClientMetrics.incrementOnError(DELETE, PODS, e);
                podsClientMetrics.registerOnErrorLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
            }
        } catch (Exception e) {
            logger.error("Failed to kill task: {} with error: ", taskId, e);
            podsClientMetrics.incrementOnError(DELETE, PODS, e);
            podsClientMetrics.registerOnErrorLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
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

    private void getNodeUpdates() {
        try {
            long startTimeMs = clock.wallTime();
            V1NodeList list = null;
            try {
                list = normalApi.listNode(null, null, null, null, null, null, null, null, null);
                nodesClientMetrics.incrementOnSuccess(GET, NODES, STATUS_200);
                nodesClientMetrics.registerOnSuccessLatency(GET, Duration.ofMillis(clock.wallTime() - startTimeMs));
            } catch (Exception e) {
                logger.error("Failed to list nodes with error:", e);
                nodesClientMetrics.incrementOnError(GET, NODES, e);
                nodesClientMetrics.registerOnErrorLatency(GET, Duration.ofMillis(clock.wallTime() - startTimeMs));
            }
            if (list != null) {
                Snapshot<V1Node> snapshot = Snapshot.newSnapshot(list.getItems(), clock.wallTime());
                SnapshotComparison<V1Node> comparison = snapshot.compare(lastSnapshot, n -> n.getMetadata().getName(), (first, second) -> true);
                //TODO check for changes to node conditions once the conditions are implemented

                List<String> leaseIdsToRemove = comparison.getRemoved().stream()
                        .map(n -> n.getMetadata().getName())
                        .collect(Collectors.toList());
                if (!leaseIdsToRemove.isEmpty() && rescindLeaseHandler != null) {
                    logger.info("Rescinding leaseIds: {}", leaseIdsToRemove);
                    List<LeaseRescindedEvent> leaseRescindedEvents = leaseIdsToRemove.stream()
                            .map(LeaseRescindedEvent::leaseIdEvent)
                            .collect(Collectors.toList());
                    rescindLeaseHandler.call(leaseRescindedEvents);
                }

                // send all leases as fenzo will currently dedup them
                List<VirtualMachineLease> leasesToAdd = snapshot.getItems().stream()
                        .map(this::nodeToLease)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                if (!leasesToAdd.isEmpty() && leaseHandler != null) {
                    logger.debug("Adding leases: {}", leasesToAdd);
                    leaseHandler.call(leasesToAdd);
                }

                lastSnapshot = snapshot;
            }
        } catch (Exception e) {
            logger.error("Error with get nodes updates: ", e);
        }
        logger.debug("Finished get node updates");
    }

    private Optional<List<V1Node>> listNodes() {
        long startTimeMs = clock.wallTime();
        V1NodeList list = null;
        try {
            list = normalApi.listNode(null, null, null, null, null, null, null, null, null);
            nodesClientMetrics.incrementOnSuccess(GET, NODES, STATUS_200);
            nodesClientMetrics.registerOnSuccessLatency(GET, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (Exception e) {
            logger.error("Failed to list nodes with error:", e);
            nodesClientMetrics.incrementOnError(GET, NODES, e);
            nodesClientMetrics.registerOnErrorLatency(GET, Duration.ofMillis(clock.wallTime() - startTimeMs));
        }
        if (list != null) {
            return Optional.of(new ArrayList<>(list.getItems()));
        }
        return Optional.empty();
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
            String nodeId = metadata.getAnnotations().getOrDefault(NODE_ID_ATTRIBUTE, nodeName);
            boolean hasTrueReadyCondition = status.getConditions().stream()
                    .anyMatch(c -> c.getType().equalsIgnoreCase(READY) && Boolean.parseBoolean(c.getStatus()));
            if (hasTrueReadyCondition) {
                return Protos.Offer.newBuilder()
                        .setId(Protos.OfferID.newBuilder().setValue(nodeId).build())
                        // TODO(fabio): change to nodeId since host IPs are ephemeral and can be reused,
                        //   but the virtual-kubelet is needs to be changed in lockstep
                        .setSlaveId(Protos.SlaveID.newBuilder().setValue(nodeName).build())
                        .setHostname(nodeName)
                        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("TitusFramework").build())
                        .addAllResources(nodeToResources(node))
                        .addAllAttributes(nodeToAttributes(node))
                        .build();
            }
        } catch (Exception ignore) {
        }
        return null;
    }

    private Iterable<? extends Protos.Resource> nodeToResources(V1Node node) {
        V1NodeStatus status = node.getStatus();
        Map<String, Quantity> allocatableResources = status.getAllocatable();
        List<Protos.Resource> resources = new ArrayList<>();
        resources.add(createResource("cpus", allocatableResources.get("cpu").getNumber().doubleValue()));
        resources.add(createResource("mem", allocatableResources.get("memory").getNumber().doubleValue()));
        resources.add(createResource("disk", allocatableResources.get("storage").getNumber().doubleValue()));
        resources.add(createResource("network", allocatableResources.get("network").getNumber().doubleValue()));
        return resources;
    }

    private Protos.Resource createResource(String name, double value) {
        return Protos.Resource.newBuilder()
                .setType(Protos.Value.Type.SCALAR)
                .setName(name)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value).build())
                .build();
    }

    private Iterable<? extends Protos.Attribute> nodeToAttributes(V1Node node) {
        V1ObjectMeta metadata = node.getMetadata();
        return metadata.getAnnotations().entrySet().stream()
                .map(e -> createAttribute(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
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

    private void getPodUpdates() {
        try {
            logger.info("Creating list namespaced pod watch");
            Watch<V1Pod> watch = Watch.createWatch(watchClient, watchApi.listNamespacedPodCall(
                    KUBERNETES_NAMESPACE, null, null, null, null,
                    null, null, null, null, true, null, null),
                    new TypeToken<Watch.Response<V1Pod>>() {
                    }.getType());
            for (Watch.Response<V1Pod> item : watch) {
                logger.info("Received pod update with type: {}, object: {}", item.type, item.object);
                V1Pod pod = item.object;
                try {
                    podUpdated(pod);
                } catch (Exception e) {
                    logger.error("Unable to handle pod update: {} with error:", pod, e);
                }
            }
        } catch (Exception e) {
            logger.error("Error with pod watch: ", e);
        } finally {
            logger.info("Finished list namespaced pod watch");
        }
    }

    private Optional<List<V1Pod>> listPods() {
        long startTimeMs = clock.wallTime();
        V1PodList list = null;
        try {
            list = normalApi.listNamespacedPod(KUBERNETES_NAMESPACE, null, null, null, null, null, null, null, null, null);
            podsClientMetrics.incrementOnSuccess(GET, PODS, STATUS_200);
            podsClientMetrics.registerOnSuccessLatency(GET, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (Exception e) {
            logger.error("Failed to list pods with error: ", e);
            podsClientMetrics.incrementOnError(GET, PODS, e);
            podsClientMetrics.registerOnErrorLatency(GET, Duration.ofMillis(clock.wallTime() - startTimeMs));
        }
        if (list != null) {
            return Optional.of(new ArrayList<>(list.getItems()));
        }
        return Optional.empty();
    }

    private void podUpdated(V1Pod pod) {

        V1ObjectMeta metadata = pod.getMetadata();
        String podName = metadata.getName();

        V1PodStatus status = pod.getStatus();
        String phase = status.getPhase();
        String reason = status.getReason();
        String reasonMessage = status.getMessage();
        boolean hasDeletionTimestamp = metadata.getDeletionTimestamp() != null;

        Optional<TitusExecutorDetails> executorDetails = getTitusExecutorDetails(pod);

        if (phase.equalsIgnoreCase(PENDING)) {
            // inspect pod status reason to differentiate between Launched and StartInitiated (this is not standard k8s)
            if (reason != null && reason.equalsIgnoreCase(TASK_STARTING)) {
                publishContainerEvent(podName, StartInitiated, REASON_NORMAL, reasonMessage, executorDetails);
            } else {
                publishContainerEvent(podName, Launched, REASON_NORMAL, reasonMessage, executorDetails);
            }
        } else if (phase.equalsIgnoreCase(RUNNING) && !hasDeletionTimestamp) {
            publishContainerEvent(podName, Started, REASON_NORMAL, reasonMessage, executorDetails);
        } else if (phase.equalsIgnoreCase(SUCCEEDED)) {
            String reasonCode = hasDeletionTimestamp ? REASON_TASK_KILLED : REASON_NORMAL;
            publishContainerEvent(podName, Finished, reasonCode, reasonMessage, executorDetails);
        } else if (phase.equalsIgnoreCase(FAILED)) {
            publishContainerEvent(podName, Finished, REASON_FAILED, reasonMessage, executorDetails);
        }
    }

    private void publishContainerEvent(String taskId, TaskState taskState, String reasonCode, String reasonMessage,
                                       Optional<TitusExecutorDetails> executorDetails) {

        if (taskState == com.netflix.titus.api.jobmanager.model.job.TaskState.Finished && !StringExt.isEmpty(reasonMessage)) {
            if (invalidRequestMessageMatcherFactory.apply(reasonMessage).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_INVALID_REQUEST;
            } else if (crashedMessageMatcherFactory.apply(reasonMessage).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_CRASHED;
            } else if (transientSystemErrorMessageMatcherFactory.apply(reasonMessage).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR;
            } else if (localSystemErrorMessageMatcherFactory.apply(reasonMessage).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_LOCAL_SYSTEM_ERROR;
            } else if (unknownSystemErrorMessageMatcherFactory.apply(reasonMessage).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
            }
        }

        V3ContainerEvent event = new V3ContainerEvent(
                taskId,
                taskState,
                reasonCode,
                reasonMessage,
                titusRuntime.getClock().wallTime(),
                executorDetails
        );
        logger.debug("Publishing task status: {}", event);
        vmTaskStatusObserver.onNext(event);
    }

    private void reconcileNodesAndPods() {
        if (!mesosConfiguration.isReconcilerEnabled()) {
            return;
        }

        Optional<List<V1Node>> nodesOpt = listNodes();
        if (!nodesOpt.isPresent()) {
            return;
        }
        List<V1Node> currentNodes = nodesOpt.get();
        List<V1Node> nodesToGc = currentNodes.stream()
                .filter(this::isNodeReadyForGc)
                .collect(Collectors.toList());

        // GC nodes that have timed out due to not publishing a heartbeat
        logger.info("Attempting to GC nodes: {}", nodesToGc);
        for (V1Node node : nodesToGc) {
            gcNode(node);
        }
        logger.info("Finished node GC");

        Map<String, Task> currentTasks = v3JobOperations.getTasks().stream().collect(Collectors.toMap(Task::getId, Function.identity()));
        Optional<List<V1Pod>> podsOpt = listPods();
        if (!podsOpt.isPresent()) {
            return;
        }
        List<V1Pod> currentPods = podsOpt.get();
        List<V1Pod> terminalPodsToGc = currentPods.stream()
                .filter(p -> {
                    Task task = currentTasks.get(p.getMetadata().getName());
                    if (task != null) {
                        return TaskState.isTerminalState(task.getStatus().getState());
                    }
                    return isPodNotRunning(p);
                })
                .collect(Collectors.toList());

        // GC pods that have been persisted in Titus and are in a terminal state
        logger.info("Attempting to GC terminal pods: {}", terminalPodsToGc);
        for (V1Pod pod : terminalPodsToGc) {
            gcPod(pod);
        }
        logger.info("Finished terminal pod GC");

        Set<String> currentNodesNames = currentNodes.stream().map(n -> n.getMetadata().getName()).collect(Collectors.toSet());
        List<V1Pod> orphanedPodsToGc = currentPods.stream()
                .filter(p -> {
                    String nodeName = p.getSpec().getNodeName();
                    return StringExt.isNotEmpty(nodeName) && !currentNodesNames.contains(nodeName);
                })
                .collect(Collectors.toList());

        // GC orphaned pods on nodes that are no longer available
        logger.info("Attempting to GC orphaned pods: {} without valid nodes", orphanedPodsToGc);
        for (V1Pod pod : orphanedPodsToGc) {
            gcPod(pod);
        }
        logger.info("Finished orphaned pod GC without valid nodes");

        List<V1Pod> pendingPodsWithDeletionTimestamp = currentPods.stream()
                .filter(pod -> {
                    DateTime deletionTimestamp = pod.getMetadata().getDeletionTimestamp();
                    return pod.getStatus().getPhase().equalsIgnoreCase(PENDING) && deletionTimestamp != null;
                })
                .collect(Collectors.toList());

        // GC pods in accepted with a deletion timestamp
        logger.info("Attempting to GC accepted pods: {} with deletion timestamp", pendingPodsWithDeletionTimestamp);
        for (V1Pod pod : pendingPodsWithDeletionTimestamp) {
            gcPod(pod);
            publishContainerEvent(pod.getMetadata().getName(), Finished, REASON_TASK_KILLED, "", Optional.empty());
        }
        logger.info("Finished accepted pods with deletion timestamp GC");

        Set<String> currentPodNames = currentPods.stream().map(p -> p.getMetadata().getName()).collect(Collectors.toSet());
        List<Task> tasksNotInApiServer = currentTasks.values().stream()
                .filter(t -> shouldTaskBeInApiServer(t) && !currentPodNames.contains(t.getId()))
                .collect(Collectors.toList());

        // Transition orphaned tasks to Finished that don't exist in Kubernetes
        logger.info("Attempting to transition orphaned tasks: {}", tasksNotInApiServer);
        for (Task task : tasksNotInApiServer) {
            publishContainerEvent(task.getId(), Finished, REASON_TASK_LOST, "Inconsistency between control plane and machine", Optional.empty());
        }
        logger.info("Finished orphaned tasks transitions");
    }

    private void gcNode(V1Node node) {
        String nodeName = node.getMetadata().getName();
        long startTimeMs = clock.wallTime();
        try {
            normalApi.deleteNode(nodeName, null, null, null, 0, null, "Background");
            nodesClientMetrics.incrementOnSuccess(DELETE, NODES, STATUS_200);
            nodesClientMetrics.registerOnSuccessLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (JsonSyntaxException e) {
            // this is probably successful. the generated client has the wrong response type
            nodesClientMetrics.incrementOnSuccess(DELETE, NODES, STATUS_200);
            nodesClientMetrics.registerOnSuccessLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (ApiException e) {
            if (!e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
                logger.error("Failed to delete node: {} with error: ", nodeName, e);
                nodesClientMetrics.incrementOnError(DELETE, NODES, e);
                nodesClientMetrics.registerOnErrorLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
            }
        } catch (Exception e) {
            logger.error("Failed to delete node: {} with error: ", nodeName, e);
            nodesClientMetrics.incrementOnError(DELETE, NODES, e);
            nodesClientMetrics.registerOnErrorLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
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
        Boolean status = Boolean.valueOf(readyCondition.getStatus());
        DateTime lastHeartbeatTime = readyCondition.getLastHeartbeatTime();
        return !status &&
                lastHeartbeatTime != null &&
                clock.isPast(lastHeartbeatTime.getMillis() + NODE_GC_TTL_MS);
    }

    private boolean isPodNotRunning(V1Pod pod) {
        return !pod.getStatus().getPhase().equalsIgnoreCase(RUNNING);
    }

    private void gcPod(V1Pod pod) {
        String podName = pod.getMetadata().getName();
        long startTimeMs = clock.wallTime();
        try {
            normalApi.deleteNamespacedPod(podName, KUBERNETES_NAMESPACE, null, null, null, 0, null, "Background");
            podsClientMetrics.incrementOnSuccess(DELETE, PODS, STATUS_200);
            podsClientMetrics.registerOnSuccessLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (JsonSyntaxException e) {
            // this is probably successful. the generated client has the wrong response type
            podsClientMetrics.incrementOnSuccess(DELETE, PODS, STATUS_200);
            podsClientMetrics.registerOnSuccessLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
        } catch (ApiException e) {
            if (!e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
                logger.error("Failed to delete pod: {} with error: ", podName, e);
                podsClientMetrics.incrementOnError(DELETE, PODS, e);
                podsClientMetrics.registerOnErrorLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
            }
        } catch (Exception e) {
            logger.error("Failed to delete pod: {} with error: ", podName, e);
            podsClientMetrics.incrementOnError(DELETE, PODS, e);
            podsClientMetrics.registerOnErrorLatency(DELETE, Duration.ofMillis(clock.wallTime() - startTimeMs));
        }
    }

    private boolean shouldTaskBeInApiServer(Task task) {
        if (!TaskState.isRunning(task.getStatus().getState())) {
            return false;
        }
        return JobFunctions.findTaskStatus(task, Launched)
                .map(s -> clock.isPast(s.getTimestamp() + ORPHANED_POD_TIMEOUT_MS))
                .orElse(false);
    }

    private Optional<TitusExecutorDetails> getTitusExecutorDetails(V1Pod pod) {
        Map<String, String> annotations = pod.getMetadata().getAnnotations();
        if (!Strings.isNullOrEmpty(annotations.get("IpAddress"))) {
            TitusExecutorDetails titusExecutorDetails = new TitusExecutorDetails(
                    Collections.emptyMap(),
                    new TitusExecutorDetails.NetworkConfiguration(
                            Boolean.parseBoolean(annotations.getOrDefault("IsRoutableIp", "true")),
                            annotations.getOrDefault("IpAddress", "UnknownIpAddress"),
                            annotations.getOrDefault("EniIpAddress", "UnknownEniIpAddress"),
                            annotations.getOrDefault("EniId", "UnknownEniId"),
                            annotations.getOrDefault("ResourceId", "UnknownResourceId")
                    )
            );
            return Optional.of(titusExecutorDetails);
        }
        return Optional.empty();
    }
}
