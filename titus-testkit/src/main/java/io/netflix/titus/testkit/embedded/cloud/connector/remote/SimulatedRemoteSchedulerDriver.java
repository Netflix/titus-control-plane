package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc.SimulatedMesosServiceBlockingStub;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc.SimulatedMesosServiceStub;
import com.netflix.titus.simulator.TitusCloudSimulator;
import com.netflix.titus.simulator.TitusCloudSimulator.Id;
import com.netflix.titus.simulator.TitusCloudSimulator.Ids;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTask;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus.SimulatedNetworkConfiguration;
import com.netflix.titus.simulator.TitusCloudSimulator.TasksLaunchRequest;
import io.grpc.ManagedChannel;
import io.netflix.titus.api.json.ObjectMappers;
import io.netflix.titus.common.grpc.GrpcUtil;
import io.netflix.titus.common.util.NetworkExt;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.master.mesos.TitusExecutorDetails;
import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

class SimulatedRemoteSchedulerDriver implements SchedulerDriver {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedRemoteSchedulerDriver.class);

    private static final Protos.FrameworkID FRAMEWORK_ID = Protos.FrameworkID.newBuilder()
            .setValue("TitusMaster@" + NetworkExt.getHostName().orElse("<host_undetermined>"))
            .build();

    private final Protos.MasterInfo masterInfo;

    private final SimulatedMesosServiceStub asyncClient;
    private final SimulatedMesosServiceBlockingStub blockingClient;

    private final Scheduler callbackHandler;

    private Subscription offerSubscription;
    private Subscription taskStatusUpdateSubscription;

    SimulatedRemoteSchedulerDriver(CloudSimulatorConnectorConfiguration configuration, ManagedChannel channel, Scheduler callbackHandler) {
        this.masterInfo = Protos.MasterInfo.newBuilder()
                .setId("MasterId#Simulated")
                .setAddress(Protos.Address.newBuilder().setHostname(configuration.getHost()).setPort(configuration.getGrpcPort()))
                .setHostname(configuration.getHost())
                .setIp(0)
                .setPort(configuration.getGrpcPort())
                .setVersion("1.2.simulated")
                .build();
        this.asyncClient = SimulatedMesosServiceGrpc.newStub(channel);
        this.blockingClient = SimulatedMesosServiceGrpc.newBlockingStub(channel);
        this.callbackHandler = callbackHandler;
    }

    @Override
    public Protos.Status start() {
        this.callbackHandler.registered(this, FRAMEWORK_ID, masterInfo);
        this.offerSubscription = GrpcUtil.toObservable(asyncClient::offerStream).subscribe(
                grpcOfferEvent -> {
                    Protos.Offer offer = toMesosOffer(grpcOfferEvent.getOffer());
                    if (grpcOfferEvent.getRescinded()) {
                        callbackHandler.offerRescinded(this, offer.getId());
                    }
                    callbackHandler.resourceOffers(this, Collections.singletonList(offer));
                },
                e -> logger.error("Offer stream terminated with an error", e),
                () -> logger.warn("Offer stream terminated")
        );
        this.taskStatusUpdateSubscription = GrpcUtil.toObservable(asyncClient::taskStatusUpdateStream).subscribe(
                taskStatus -> callbackHandler.statusUpdate(this, toTaskStatusUpdate(taskStatus)),
                e -> logger.error("Offer stream terminated with an error", e),
                () -> logger.warn("Offer stream terminated")
        );
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status stop(boolean failover) {
        return stop();
    }

    @Override
    public Protos.Status stop() {
        ObservableExt.safeUnsubscribe(offerSubscription, taskStatusUpdateSubscription);
        return Protos.Status.DRIVER_STOPPED;
    }

    @Override
    public Protos.Status abort() {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status join() {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status run() {
        start();
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status requestResources(Collection<Protos.Request> requests) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        return launchTasks(offerIds, tasks);
    }

    @Override
    public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks) {
        TasksLaunchRequest request = TasksLaunchRequest.newBuilder()
                .addAllOfferIds(offerIds.stream().map(Protos.OfferID::getValue).collect(Collectors.toList()))
                .addAllTasks(tasks.stream().map(this::toSimulatedTask).collect(Collectors.toList()))
                .build();
        blockingClient.launchTasks(request);
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        return launchTasks(Collections.singleton(offerId), tasks);
    }

    @Override
    public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks) {
        return launchTasks(Collections.singleton(offerId), tasks);
    }

    @Override
    public Protos.Status killTask(Protos.TaskID taskId) {
        blockingClient.killTask(Id.newBuilder().setId(taskId.getValue()).build());
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status acceptOffers(Collection<Protos.OfferID> offerIds, Collection<Protos.Offer.Operation> operations, Protos.Filters filters) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status declineOffer(Protos.OfferID offerId, Protos.Filters filters) {
        return declineOffer(offerId);
    }

    @Override
    public Protos.Status declineOffer(Protos.OfferID offerId) {
        blockingClient.declineOffer(Id.newBuilder().setId(offerId.getValue()).build());
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status reviveOffers() {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status suppressOffers() {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status acknowledgeStatusUpdate(Protos.TaskStatus status) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status sendFrameworkMessage(Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        throw new IllegalStateException("method not supported");
    }

    @Override
    public Protos.Status reconcileTasks(Collection<Protos.TaskStatus> statuses) {
        List<String> taskIds = statuses.stream().map(s -> s.getTaskId().getValue()).collect(Collectors.toList());
        blockingClient.reconcile(Ids.newBuilder().addAllIds(taskIds).build());
        return Protos.Status.DRIVER_RUNNING;
    }

    private Protos.Offer toMesosOffer(TitusCloudSimulator.SimulatedOffer offer) {
        TitusCloudSimulator.SimulatedComputeResources resources = offer.getResources();

        List<Protos.Attribute> attributes = offer.getAttributesMap().entrySet().stream()
                .map(entry -> Protos.Attribute.newBuilder()
                        .setName(entry.getKey())
                        .setText(Protos.Value.Text.newBuilder().setValue(entry.getValue()))
                        .setType(Protos.Value.Type.SCALAR)
                        .build()
                ).collect(Collectors.toList());

        return Protos.Offer.newBuilder()
                .setFrameworkId(FRAMEWORK_ID)
                .setId(Protos.OfferID.newBuilder().setValue(offer.getOfferId()).build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(offer.getInstanceId()).build())
                .setHostname(offer.getInstanceHostname())
                .addResources(toMesosResource("cpus", resources.getCpu()))
                .addResources(toMesosResource("gpu", resources.getGpu()))
                .addResources(toMesosResource("mem", resources.getMemoryMB()))
                .addResources(toMesosResource("network", resources.getNetworkMB()))
                .addResources(toMesosResource("disk", resources.getMemoryMB()))
                .addAllAttributes(attributes)
                .build();
    }

    private Protos.Resource toMesosResource(String name, int value) {
        return Protos.Resource.newBuilder()
                .setName(name)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
                .build();
    }

    private SimulatedTask toSimulatedTask(Protos.TaskInfo taskInfo) {
        TitanProtos.ContainerInfo containerInfo;
        try {
            containerInfo = TitanProtos.ContainerInfo.parseFrom(taskInfo.getData());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException(e);
        }

        return SimulatedTask.newBuilder()
                .setTaskId(taskInfo.getTaskId().getValue())
                .setInstanceId(taskInfo.getSlaveId().getValue())
                .setAllocateIpAddress(containerInfo.getAllocateIpAddress())
                .setComputeResources(TitusCloudSimulator.SimulatedComputeResources.newBuilder()
                        .setCpu(getResourceValue("cpus", taskInfo))
                        .setGpu(getResourceValue("gpu", taskInfo))
                        .setMemoryMB(getResourceValue("mem", taskInfo))
                        .setDiskMB(getResourceValue("disk", taskInfo))
                        .setNetworkMB(getResourceValue("network", taskInfo))
                )
                .setEniLabel(containerInfo.getNetworkConfigInfo().getEniLabel())
                .putAllEnv(containerInfo.getTitusProvidedEnvMap())
                .build();
    }

    private int getResourceValue(String name, Protos.TaskInfo taskInfo) {
        return taskInfo.getResourcesList().stream()
                .filter(r -> r.getName().equals(name))
                .findFirst()
                .map(r -> (int) r.getScalar().getValue())
                .orElse(0);
    }

    private Protos.TaskStatus toTaskStatusUpdate(SimulatedTaskStatus simulatedTaskStatus) {
        Protos.TaskStatus.Builder mesosStatusBuilder = Protos.TaskStatus.newBuilder();

        mesosStatusBuilder
                .setTaskId(Protos.TaskID.newBuilder().setValue(simulatedTaskStatus.getTaskId()))
                .setMessage(simulatedTaskStatus.getMessage())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(simulatedTaskStatus.getInstanceId()));

        switch (simulatedTaskStatus.getTaskState()) {
            case Launched:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_STAGING);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.valueOf(0));
                break;
            case StartInitiated:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_STARTING);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.valueOf(0));
                break;
            case Started:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_RUNNING);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.valueOf(0));
                break;
            case Killed:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_KILLED);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.valueOf(0));
                break;
            case Finished:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_FINISHED);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.valueOf(0));
                break;
            case Failed:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_FAILED);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.REASON_COMMAND_EXECUTOR_FAILED);
                break;
            case Lost:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_LOST);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.REASON_TASK_UNKNOWN);
                break;
            case UNRECOGNIZED:
            default:
                throw new IllegalArgumentException("Unexpected task state: " + simulatedTaskStatus.getTaskState());
        }

        SimulatedNetworkConfiguration networkConfiguration = simulatedTaskStatus.getNetworkConfiguration();
        if (networkConfiguration != SimulatedNetworkConfiguration.getDefaultInstance()) {
            networkConfiguration.getIpAddress();
            TitusExecutorDetails executorDetails = new TitusExecutorDetails(
                    Collections.singletonMap("nfvpc", networkConfiguration.getIpAddress()),
                    new TitusExecutorDetails.NetworkConfiguration(
                            true,
                            networkConfiguration.getIpAddress(),
                            networkConfiguration.getEniIPAddress(),
                            networkConfiguration.getEniID(),
                            networkConfiguration.getResourceID()
                    )
            );
            try {
                String data = ObjectMappers.compactMapper().writeValueAsString(executorDetails);
                mesosStatusBuilder.setData(ByteString.copyFrom(data, Charset.defaultCharset()));
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }

        return mesosStatusBuilder.build();
    }
}
