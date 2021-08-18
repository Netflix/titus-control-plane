/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded.cloud.connector.remote;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.NetworkExt;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.rx.RetryHandlerBuilder;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
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
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;
import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Subscription;
import rx.schedulers.Schedulers;

class SimulatedRemoteMesosSchedulerDriver implements SchedulerDriver {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedRemoteMesosSchedulerDriver.class);

    private static final Protos.FrameworkID FRAMEWORK_ID = Protos.FrameworkID.newBuilder()
            .setValue("TitusMaster@" + NetworkExt.getHostName().orElse("<host_undetermined>"))
            .build();

    private final Protos.MasterInfo masterInfo;

    private final SimulatedMesosServiceStub asyncClient;
    private final SimulatedMesosServiceBlockingStub blockingClient;

    private final Scheduler callbackHandler;
    private final TitusRuntime titusRuntime;

    private Subscription offerSubscription;
    private Subscription taskStatusUpdateSubscription;

    private final BlockingQueue<TasksLaunchRequest> launchQueue = new LinkedBlockingQueue<>();
    private Subscription queueDrainerSubscription;

    SimulatedRemoteMesosSchedulerDriver(Protos.MasterInfo masterInfo, Channel channel, Scheduler callbackHandler, TitusRuntime titusRuntime) {
        this.masterInfo = masterInfo;
        this.asyncClient = SimulatedMesosServiceGrpc.newStub(channel);
        this.blockingClient = SimulatedMesosServiceGrpc.newBlockingStub(channel);
        this.callbackHandler = callbackHandler;
        this.titusRuntime = titusRuntime;
    }

    @Override
    public Protos.Status start() {
        this.callbackHandler.registered(this, FRAMEWORK_ID, masterInfo);

        this.queueDrainerSubscription = ObservableExt.schedule(
                "titusMaster.simulatedRemoteMesosSchedulerDriver",
                titusRuntime.getRegistry(),
                "queueDrainer",
                drainRequestQueues(),
                0,
                1,
                TimeUnit.MILLISECONDS,
                Schedulers.computation()
        ).subscribe(
                next -> next.ifPresent(error -> logger.warn("Queue processing error", error)),
                e -> logger.error("Queue drainer process died", e),
                () -> logger.info("Queue drainer process completed")
        );

        this.offerSubscription = GrpcUtil.toObservable(asyncClient::offerStream)
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(100, 1_000, TimeUnit.MILLISECONDS)
                        .buildExponentialBackoff()
                ).subscribe(
                        grpcOfferEvent -> {
                            Protos.Offer offer = toMesosOffer(grpcOfferEvent.getOffer());
                            if (grpcOfferEvent.getRescinded()) {
                                callbackHandler.offerRescinded(this, offer.getId());
                            } else {
                                callbackHandler.resourceOffers(this, Collections.singletonList(offer));
                            }
                        },
                        e -> logger.error("Offer stream terminated with an error", e),
                        () -> logger.warn("Offer stream terminated")
                );
        this.taskStatusUpdateSubscription = GrpcUtil.toObservable(asyncClient::taskStatusUpdateStream)
                .retryWhen(RetryHandlerBuilder.retryHandler()
                        .withUnlimitedRetries()
                        .withDelay(100, 1_000, TimeUnit.MILLISECONDS)
                        .buildExponentialBackoff()
                ).subscribe(
                        taskStatus -> {
                            long startTime = System.currentTimeMillis();
                            callbackHandler.statusUpdate(this, toTaskStatusUpdate(taskStatus));
                            logger.info("Forwarded task status update: taskId={}, newStatus={}, elapsedMs={}",
                                    taskStatus.getTaskId(), taskStatus.getTaskState(), System.currentTimeMillis() - startTime);
                        },
                        e -> logger.error("Task status stream terminated with an error", e),
                        () -> logger.warn("Task status stream terminated")
                );
        return Protos.Status.DRIVER_RUNNING;
    }

    private Completable drainRequestQueues() {
        return Completable.fromEmitter(emitter -> {
            if (launchQueue.isEmpty()) {
                emitter.onCompleted();
                return;
            }
            List<TasksLaunchRequest> batch = new ArrayList<>();
            launchQueue.drainTo(batch);

            TitusCloudSimulator.TasksLaunchRequests request = TitusCloudSimulator.TasksLaunchRequests.newBuilder().addAllLaunchRequests(batch).build();
            asyncClient.launchTasks(request, new StreamObserver<Empty>() {
                @Override
                public void onNext(Empty value) {
                }

                @Override
                public void onError(Throwable t) {
                    emitter.onError(t);
                }

                @Override
                public void onCompleted() {
                    emitter.onCompleted();
                }
            });
        });
    }

    @Override
    public Protos.Status stop(boolean failover) {
        return stop();
    }

    @Override
    public Protos.Status stop() {
        ObservableExt.safeUnsubscribe(offerSubscription, taskStatusUpdateSubscription, queueDrainerSubscription);
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
        logger.info("Launching tasks: taskIds={}, offerIds={}",
                tasks.stream().map(t -> t.getTaskId().getValue()).collect(Collectors.toList()),
                offerIds.stream().map(Protos.OfferID::getValue).collect(Collectors.toList())
        );

        TasksLaunchRequest request = TasksLaunchRequest.newBuilder()
                .addAllOfferIds(offerIds.stream().map(Protos.OfferID::getValue).collect(Collectors.toList()))
                .addAllTasks(tasks.stream().map(this::toSimulatedTask).collect(Collectors.toList()))
                .build();

        launchQueue.add(request);

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
        logger.info("Killing task: {}", taskId.getValue());
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
        logger.info("Declining offer: {}", offerId.getValue());
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

        Map<String, String> completeEnv = new HashMap<>(containerInfo.getTitusProvidedEnvMap());
        completeEnv.putAll(containerInfo.getUserProvidedEnvMap());

        return SimulatedTask.newBuilder()
                .setTaskId(taskInfo.getTaskId().getValue())
                .setInstanceId(taskInfo.getSlaveId().getValue())
                .setComputeResources(TitusCloudSimulator.SimulatedComputeResources.newBuilder()
                        .setCpu(getResourceValue("cpus", taskInfo))
                        .setGpu(getResourceValue("gpu", taskInfo))
                        .setMemoryMB(getResourceValue("mem", taskInfo))
                        .setDiskMB(getResourceValue("disk", taskInfo))
                        .setNetworkMB(getResourceValue("network", taskInfo))
                )
                .setEniLabel(containerInfo.getNetworkConfigInfo().getEniLabel())
                .putAllEnv(completeEnv)
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
            case KillInitiated:
                mesosStatusBuilder.setState(Protos.TaskState.TASK_KILLING);
                mesosStatusBuilder.setReason(Protos.TaskStatus.Reason.REASON_TASK_KILLED_DURING_LAUNCH);
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
                            null,
                            networkConfiguration.getEniIPAddress(),
                            "UnknownNetworkMode",
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
