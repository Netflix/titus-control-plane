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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.JsonSyntaxException;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.annotation.Experimental;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.network.client.ClientMetrics;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.ExecutorsExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.mesos.ContainerEvent;
import com.netflix.titus.master.mesos.LeaseRescindedEvent;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.master.mesos.V3ContainerEvent;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.models.V1Container;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Node;
import io.kubernetes.client.models.V1NodeAddress;
import io.kubernetes.client.models.V1NodeList;
import io.kubernetes.client.models.V1NodeStatus;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.models.V1PodSpec;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.models.V1ResourceRequirements;
import io.kubernetes.client.util.Config;
import org.apache.mesos.Protos;
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

/**
 * Responsible for integrating Kubernetes API Server concepts into Titus's Mesos based approaches.
 */
@Experimental(detail = "This is a basic integration with the kubernetes api server", deadline = "7/1/2019")
@Singleton
public class KubeApiServerIntegrator implements VirtualMachineMasterService {
    private static final Logger logger = LoggerFactory.getLogger(KubeApiServerIntegrator.class);
    private static final String ATTRIBUTE_PREFIX = "com.netflix.titus.agent.attribute/";
    private static final String KUBERNETES_NAMESPACE = "default";
    private static final String CLIENT_METRICS_PREFIX = "titusMaster.mesos.kubeApiServerIntegration";
    private static final int GRACE_PERIOD_SECONDS = 300;

    private static final String POST = "POST";
    private static final String GET = "GET";
    private static final String DELETE = "DELETE";
    private static final String STATUS_200 = "200";
    private static final String NODES = "nodes";
    private static final String PODS = "pods";

    private final TitusRuntime titusRuntime;
    private final MesosConfiguration mesosConfiguration;
    private final LocalScheduler scheduler;
    private final Clock clock;

    private final ClientMetrics nodesClientMetrics;
    private final ClientMetrics podsClientMetrics;
    private final Cache<String, V1Pod> lastPodUpdate = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES)
            .build();

    private com.netflix.fenzo.functions.Action1<List<? extends VirtualMachineLease>> leaseHandler;
    private Subject<LeaseRescindedEvent, LeaseRescindedEvent> vmLeaseRescindedObserver;
    private Subject<ContainerEvent, ContainerEvent> vmTaskStatusObserver;
    private CoreV1Api api;

    @Inject
    public KubeApiServerIntegrator(TitusRuntime titusRuntime,
                                   MesosConfiguration mesosConfiguration,
                                   LocalScheduler scheduler) {
        this.titusRuntime = titusRuntime;
        this.mesosConfiguration = mesosConfiguration;
        this.scheduler = scheduler;
        this.clock = titusRuntime.getClock();

        this.vmLeaseRescindedObserver = PublishSubject.create();
        this.vmTaskStatusObserver = PublishSubject.create();

        nodesClientMetrics = new ClientMetrics(CLIENT_METRICS_PREFIX, NODES, titusRuntime.getRegistry(), clock);
        podsClientMetrics = new ClientMetrics(CLIENT_METRICS_PREFIX, PODS, titusRuntime.getRegistry(), clock);
    }

    @Override
    public void enterActiveMode() {
        ApiClient client = Config.fromUrl(mesosConfiguration.getKubeApiServerUrl());
        Configuration.setDefaultApiClient(client);
        api = new CoreV1Api(client);

        ScheduleDescriptor getNodeSchedulerDescriptor = ScheduleDescriptor.newBuilder()
                .withName("getNodes")
                .withDescription("Get node information")
                .withInterval(Duration.ofSeconds(5))
                .withTimeout(Duration.ofSeconds(10))
                .build();
        scheduler.schedule(getNodeSchedulerDescriptor, e -> pollForNodes(), ExecutorsExt.namedSingleThreadExecutor("kube-api-server-integrator-nodes"));

        ScheduleDescriptor getPodsSchedulerDescriptor = ScheduleDescriptor.newBuilder()
                .withName("getPods")
                .withDescription("Get pod information")
                .withInterval(Duration.ofSeconds(5))
                .withTimeout(Duration.ofSeconds(10))
                .build();
        scheduler.schedule(getPodsSchedulerDescriptor, e -> pollForPods(), ExecutorsExt.namedSingleThreadExecutor("kube-api-server-integrator-pods"));
    }

    @Override
    public void launchTasks(List<Protos.TaskInfo> requests, List<VirtualMachineLease> leases) {
        for (Protos.TaskInfo request : requests) {
            long startTimeMs = clock.wallTime();
            try {
                V1Pod v1Pod = taskInfoToPod(request);
                logger.debug("pod: {}", v1Pod);
                api.createNamespacedPod(KUBERNETES_NAMESPACE, v1Pod, false, null, null);
                podsClientMetrics.incrementOnSuccess(POST, PODS, STATUS_200);
            } catch (ApiException e) {
                logger.error("Unable to create pod with error:", e);
                podsClientMetrics.incrementOnError(POST, PODS, e);
            } finally {
                podsClientMetrics.registerLatency(POST, startTimeMs);
            }
        }
    }

    private V1Pod taskInfoToPod(Protos.TaskInfo taskInfo) {
        String taskId = taskInfo.getName();
        String nodeName = taskInfo.getSlaveId().getValue();
        String encodedContainerInfo = Base64.getEncoder().encodeToString(taskInfo.getData().toByteArray());

        V1ObjectMeta metadata = new V1ObjectMeta()
                .name(taskId)
                .annotations(Collections.singletonMap("containerInfo", encodedContainerInfo));

        V1Container container = new V1Container()
                .name(taskId)
                .image("imageIsInContainerInfo")
                .resources(taskInfoToResources(taskInfo));

        V1PodSpec spec = new V1PodSpec()
                .nodeName(nodeName)
                .containers(Collections.singletonList(container))
                .terminationGracePeriodSeconds(600L);

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
        // do nothing since there isn't a real lease
    }

    @Override
    public void killTask(String taskId) {
        long startTimeMs = clock.wallTime();
        try {
            api.deleteNamespacedPod(taskId, KUBERNETES_NAMESPACE, new V1DeleteOptions(), null, null, GRACE_PERIOD_SECONDS, null, null);
            podsClientMetrics.incrementOnSuccess(DELETE, PODS, STATUS_200);
        } catch (JsonSyntaxException e) {
            // this is probably successful. the generated client has the wrong response type
            podsClientMetrics.incrementOnSuccess(DELETE, PODS, STATUS_200);
        } catch (ApiException e) {
            if (e.getMessage().equalsIgnoreCase("Not found")) {
                // pod is no longer in api server
                publishContainerEvent(taskId, Finished, REASON_NORMAL, "", Optional.empty());
            } else {
                logger.error("Failed to kill task: {} with error: ", taskId, e);
                podsClientMetrics.incrementOnError(DELETE, PODS, e);
            }
        } catch (Exception e) {
            logger.error("Failed to kill task: {} with error: ", taskId, e);
            podsClientMetrics.incrementOnError(DELETE, PODS, e);
        } finally {
            podsClientMetrics.registerLatency(DELETE, startTimeMs);
        }
    }

    @Override
    public void setVMLeaseHandler(Action1<List<? extends VirtualMachineLease>> leaseHandler) {
        this.leaseHandler = leaseHandler;
    }

    @Override
    public Observable<LeaseRescindedEvent> getLeaseRescindedObservable() {
        return vmLeaseRescindedObserver.asObservable();
    }

    @Override
    public Observable<ContainerEvent> getTaskStatusObservable() {
        return vmTaskStatusObserver.asObservable();
    }

    private void pollForNodes() {
        long startTimeMs = clock.wallTime();
        V1NodeList list = null;
        try {
            list = api.listNode(null, null, null, null, null, null, null, null, null);
            nodesClientMetrics.incrementOnSuccess(GET, NODES, STATUS_200);
        } catch (Exception e) {
            logger.error("Failed to list nodes with error:", e);
            nodesClientMetrics.incrementOnError(GET, NODES, e);
        } finally {
            nodesClientMetrics.registerLatency(GET, startTimeMs);
        }
        if (list != null && !list.getItems().isEmpty()) {
            final List<VMLeaseObject> leaseObjects = list.getItems().stream()
                    .map(this::nodeToOffer)
                    .filter(Objects::nonNull)
                    .map(VMLeaseObject::new)
                    .collect(Collectors.toList());
            if (!leaseObjects.isEmpty()) {
                vmLeaseRescindedObserver.onNext(LeaseRescindedEvent.allEvent());
                leaseHandler.call(leaseObjects);
            }
        }
    }

    private Protos.Offer nodeToOffer(V1Node node) {
        try {
            String offerId = UUID.randomUUID().toString();
            V1ObjectMeta metadata = node.getMetadata();
            V1NodeStatus status = node.getStatus();
            boolean hasTrueReadyCondition = status.getConditions().stream()
                    .anyMatch(c -> c.getType().equalsIgnoreCase("Ready") && c.getStatus().equalsIgnoreCase("True"));
            if (hasTrueReadyCondition) {
                String hostname = status.getAddresses().stream().filter(a -> a.getType().equals("InternalIP"))
                        .findFirst()
                        .map(V1NodeAddress::getAddress)
                        .orElse("");

                return Protos.Offer.newBuilder()
                        .setId(Protos.OfferID.newBuilder().setValue(offerId).build())
                        .setHostname(hostname)
                        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("TitusFramework").build())
                        .setSlaveId(Protos.SlaveID.newBuilder().setValue(metadata.getName()).build())
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

    private void pollForPods() {
        long startTimeMs = clock.wallTime();
        V1PodList list = null;
        try {
            list = api.listNamespacedPod(KUBERNETES_NAMESPACE, null, null, null, null, null, null, null, null, null);
            podsClientMetrics.incrementOnSuccess(GET, PODS, STATUS_200);
        } catch (Exception e) {
            logger.error("Failed to list pods with error: ", e);
            podsClientMetrics.incrementOnError(GET, PODS, e);
        } finally {
            podsClientMetrics.registerLatency(GET, startTimeMs);
        }
        if (list != null && !list.getItems().isEmpty()) {
            for (V1Pod pod : list.getItems()) {
                String taskId = pod.getMetadata().getName();
                V1Pod lastPod = lastPodUpdate.getIfPresent(taskId);
                if (!Objects.equals(pod, lastPod)) {
                    try {
                        podUpdated(pod);
                    } catch (Exception ignored) {
                    }
                    lastPodUpdate.put(taskId, pod);
                }
            }
        }
    }

    private void podUpdated(V1Pod pod) {
        String taskId = pod.getMetadata().getName();
        V1PodStatus status = pod.getStatus();
        String phase = status.getPhase();

        if (phase.equalsIgnoreCase("Pending")) {
            publishContainerEvent(taskId, Launched, REASON_NORMAL, status.getReason(), Optional.empty());
        } else if (phase.equalsIgnoreCase("Running")) {
            TitusExecutorDetails titusExecutorDetails = new TitusExecutorDetails(
                    Collections.emptyMap(),
                    new TitusExecutorDetails.NetworkConfiguration(
                            true,
                            status.getPodIP(),
                            "UnknownEniIpAddress",
                            "unknownEniId",
                            "unknownResourceId"
                    )
            );
            publishContainerEvent(taskId, StartInitiated, REASON_NORMAL, status.getReason(), Optional.of(titusExecutorDetails));
            publishContainerEvent(taskId, Started, REASON_NORMAL, status.getReason(), Optional.empty());
        } else if (phase.equalsIgnoreCase("Succeeded")) {
            publishContainerEvent(taskId, Finished, REASON_NORMAL, status.getReason(), Optional.empty());
        } else if (phase.equalsIgnoreCase("Failed")) {
            publishContainerEvent(taskId, Started, REASON_FAILED, status.getReason(), Optional.empty());
        }
    }

    private void publishContainerEvent(String taskId, TaskState taskState, String reasonCode, String reasonMessage,
                                       Optional<TitusExecutorDetails> executorDetails) {
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
}
