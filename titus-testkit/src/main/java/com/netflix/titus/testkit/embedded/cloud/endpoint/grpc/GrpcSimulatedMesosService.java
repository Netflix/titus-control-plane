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

package com.netflix.titus.testkit.embedded.cloud.endpoint.grpc;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
import com.netflix.titus.master.mesos.TitusExecutorDetails;
import com.netflix.titus.runtime.endpoint.common.grpc.GrpcUtil;
import com.netflix.titus.simulator.SimulatedMesosServiceGrpc.SimulatedMesosServiceImplBase;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedComputeResources;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedOfferEvent;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTask;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus.SimulatedNetworkConfiguration;
import com.netflix.titus.simulator.TitusCloudSimulator.TasksLaunchRequests;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.agent.OfferChangeEvent;
import io.grpc.stub.StreamObserver;
import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscription;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;
import static com.netflix.titus.simulator.TitusCloudSimulator.Id;
import static com.netflix.titus.simulator.TitusCloudSimulator.Ids;
import static com.netflix.titus.simulator.TitusCloudSimulator.SimulatedOffer;
import static com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus;
import static com.netflix.titus.simulator.TitusCloudSimulator.TasksLaunchRequest;

@Singleton
public class GrpcSimulatedMesosService extends SimulatedMesosServiceImplBase {

    private static final Logger logger = LoggerFactory.getLogger(GrpcSimulatedMesosService.class);

    private final SimulatedCloud cloud;

    @Inject
    public GrpcSimulatedMesosService(SimulatedCloud cloud) {
        this.cloud = cloud;
    }

    @Override
    public void offerStream(Empty request, StreamObserver<SimulatedOfferEvent> responseObserver) {
        Subscription subscription = cloud.offers().subscribe(
                offerEvent -> {
                    logger.info("Sending new offer event: offerId={}, rescinded={}", offerEvent.getOffer().getId().getValue(), offerEvent.isRescind());
                    responseObserver.onNext(toSimulatedOfferEvent(offerEvent));
                },
                e -> {
                    logger.info("Offer subscription stream terminated with an error: {}", e.getMessage(), e);
                    responseObserver.onError(e);
                },
                () -> {
                    logger.info("Offer subscription stream completed");
                    responseObserver.onCompleted();
                }
        );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void declineOffer(Id request, StreamObserver<Empty> responseObserver) {
        cloud.declineOffer(request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void taskStatusUpdateStream(Empty request, StreamObserver<SimulatedTaskStatus> responseObserver) {
        Subscription subscription = cloud.taskStatusUpdates().subscribe(
                statusUpdate -> {
                    logger.info("Sending task status update: taskId={}, state={}", statusUpdate.getTaskId().getValue(), statusUpdate.getState());
                    responseObserver.onNext(toSimulatedStatusUpdate(statusUpdate));
                },
                e -> {
                    logger.info("Task subscription stream terminated with an error", e);
                    responseObserver.onError(e);
                },
                () -> {
                    logger.info("Task subscription stream terminated");
                    responseObserver.onCompleted();
                }
        );
        GrpcUtil.attachCancellingCallback(responseObserver, subscription);
    }

    @Override
    public void launchTasks(TasksLaunchRequests requests, StreamObserver<Empty> responseObserver) {
        for (TasksLaunchRequest request : requests.getLaunchRequestsList()) {
            List<Protos.TaskInfo> taskInfos = request.getTasksList().stream()
                    .map(GrpcSimulatedMesosService::toMesosTaskInfo)
                    .collect(Collectors.toList());
            cloud.launchTasks(request.getOfferIdsList(), taskInfos);
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void killTask(Id request, StreamObserver<Empty> responseObserver) {
        cloud.killTask(request.getId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void reconcile(Ids request, StreamObserver<Empty> responseObserver) {
        if (request.getIdsList().isEmpty()) {
            cloud.reconcileKnownTasks();
        } else {
            cloud.reconcileTasks(new HashSet<>(request.getIdsList()));
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    private SimulatedOfferEvent toSimulatedOfferEvent(OfferChangeEvent offerEvent) {
        Protos.Offer offer = offerEvent.getOffer();

        SimulatedComputeResources resources = SimulatedComputeResources.newBuilder()
                .setCpu(getResourceScalarValue("cpus", offer))
                .setGpu(getResourceScalarValue("gpu", offer))
                .setMemoryMB(getResourceScalarValue("mem", offer))
                .setDiskMB(getResourceScalarValue("disk", offer))
                .setNetworkMB(getResourceScalarValue("network", offer))
                .build();

        Map<String, String> attributes = new HashMap<>();
        offer.getAttributesList().forEach(a -> attributes.put(a.getName(), a.getText().getValue()));

        return SimulatedOfferEvent.newBuilder()
                .setOffer(SimulatedOffer.newBuilder()
                        .setOfferId(offer.getId().getValue())
                        .setInstanceId(offer.getSlaveId().getValue())
                        .setInstanceHostname(offer.getHostname())
                        .setResources(resources)
                        .putAllAttributes(attributes)
                )
                .setRescinded(offerEvent.isRescind())
                .build();
    }

    private int getResourceScalarValue(String name, Protos.Offer offer) {
        return offer.getResourcesList().stream()
                .filter(r -> r.getName().equals(name))
                .findFirst()
                .map(r -> (int) r.getScalar().getValue())
                .orElse(0);
    }

    private SimulatedTaskStatus toSimulatedStatusUpdate(Protos.TaskStatus statusUpdate) {
        return SimulatedTaskStatus.newBuilder()
                .setTaskId(statusUpdate.getTaskId().getValue())
                .setInstanceId(statusUpdate.getSlaveId().getValue())
                .setTaskState(toSimulatedTaskState(statusUpdate.getState()))
                .setMessage(statusUpdate.getMessage())
                .setNetworkConfiguration(toSimulatedNetworkConfiguration(statusUpdate.getData()))
                .build();
    }

    private SimulatedNetworkConfiguration toSimulatedNetworkConfiguration(ByteString data) {
        if (data == null) {
            return SimulatedNetworkConfiguration.getDefaultInstance();
        }
        return JobManagerUtil.parseDetails(data.toString(Charset.defaultCharset()))
                .map(n -> {
                            TitusExecutorDetails.NetworkConfiguration networkConfiguration = n.getNetworkConfiguration();
                            return SimulatedNetworkConfiguration.newBuilder()
                                    .setIsRoutableIP(networkConfiguration.isRoutableIP())
                                    .setEniID(getOrDefault(networkConfiguration.getEniID(), ""))
                                    .setEniIPAddress(getOrDefault(networkConfiguration.getEniIPAddress(), ""))
                                    .setIpAddress(getOrDefault(networkConfiguration.getIpAddress(), ""))
                                    .setResourceID(getOrDefault(networkConfiguration.getResourceID(), ""))
                                    .build();
                        }
                ).orElse(SimulatedNetworkConfiguration.getDefaultInstance());
    }

    private SimulatedTaskStatus.SimulatedTaskState toSimulatedTaskState(Protos.TaskState mesosState) {
        switch (mesosState) {
            case TASK_STAGING:
                return SimulatedTaskStatus.SimulatedTaskState.Launched;
            case TASK_STARTING:
                return SimulatedTaskStatus.SimulatedTaskState.StartInitiated;
            case TASK_RUNNING:
                return SimulatedTaskStatus.SimulatedTaskState.Started;
            case TASK_KILLING:
                return SimulatedTaskStatus.SimulatedTaskState.KillInitiated;
            case TASK_FINISHED:
                return SimulatedTaskStatus.SimulatedTaskState.Finished;
            case TASK_FAILED:
                return SimulatedTaskStatus.SimulatedTaskState.Failed;
            case TASK_KILLED:
                return SimulatedTaskStatus.SimulatedTaskState.Killed;
            case TASK_LOST:
                return SimulatedTaskStatus.SimulatedTaskState.Lost;
            case TASK_ERROR:
                return SimulatedTaskStatus.SimulatedTaskState.Failed;
            case TASK_UNREACHABLE:
            case TASK_GONE:
            case TASK_DROPPED:
            case TASK_GONE_BY_OPERATOR:
            case TASK_UNKNOWN:
        }
        return SimulatedTaskStatus.SimulatedTaskState.UNRECOGNIZED;
    }

    private static Protos.TaskInfo toMesosTaskInfo(SimulatedTask task) {
        SimulatedComputeResources computeResources = task.getComputeResources();
        List<Protos.Resource> resources = Arrays.asList(
                newResource("cpus", computeResources.getCpu()),
                newResource("mem", computeResources.getMemoryMB()),
                newResource("disk", computeResources.getDiskMB()),
                newResource("network", computeResources.getNetworkMB())
        );

        TitanProtos.ContainerInfo containerInfo = TitanProtos.ContainerInfo.newBuilder()
                .setAllocateIpAddress(task.getAllocateIpAddress())
                .setNetworkConfigInfo(TitanProtos.ContainerInfo.NetworkConfigInfo.newBuilder()
                        .setAllocateIpAddress(task.getAllocateIpAddress())
                        .setEniLabel(task.getEniLabel())
                        .setEniLablel(task.getEniLabel())
                        .addAllSecurityGroups(task.getSecurityGroupsList())
                )
                .putAllTitusProvidedEnv(task.getEnvMap())
                .build();

        return Protos.TaskInfo.newBuilder()
                .setName(task.getTaskId())
                .setTaskId(Protos.TaskID.newBuilder().setValue(task.getTaskId()))
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(task.getInstanceId()))
                .addAllResources(resources)
                .setData(containerInfo.toByteString())
                .build();
    }

    private static Protos.Resource newResource(String name, int value) {
        return Protos.Resource.newBuilder().setName(name).setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(value)).build();
    }
}
