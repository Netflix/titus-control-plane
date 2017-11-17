/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.embedded.cloud.agent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.mesos.MesosSchedulerCallbackHandler;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.util.PrettyPrinters;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.subjects.BehaviorSubject;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

/**
 * A stub implementation of {@link SchedulerDriver} to simulate Mesos framework and a collection of agent/resources.
 * The implementation is minimalistic, and covers only features used by Titus master.
 */
public class SimulatedMesosSchedulerDriver implements SchedulerDriver {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedMesosSchedulerDriver.class);

    private final AtomicReference<Status> statusRef = new AtomicReference<>(Status.DRIVER_NOT_STARTED);
    private final SimulatedCloud simulatedCloud;
    private final Protos.FrameworkInfo framework;
    private final Protos.MasterInfo masterInfo;
    private final MesosSchedulerCallbackHandler scheduler;

    private final BehaviorSubject<Status> driverStatusUpdates = BehaviorSubject.create();
    private final Subject<OfferChangeEvent, OfferChangeEvent> offerUpdates = new SerializedSubject<>(PublishSubject.create());
    private final Subject<Protos.TaskStatus, Protos.TaskStatus> taskStatusUpdates = new SerializedSubject<>(PublishSubject.create());
    private final PublishSubject<TaskExecutorHolder> launchedTaskUpdates = PublishSubject.create();

    private final ConcurrentMap<Protos.SlaveID, ConnectedAgent> connectedAgents = new ConcurrentHashMap<>();
    private final ConcurrentMap<Protos.OfferID, Protos.Offer> offeredResources = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TaskExecutorHolder> launchedTasks = new ConcurrentHashMap<>();

    private Subscription offerUpdateSubscription;
    private Subscription taskStatusSubscription;

    public SimulatedMesosSchedulerDriver(SimulatedCloud simulatedCloud,
                                         Protos.FrameworkInfo framework,
                                         MesosSchedulerCallbackHandler scheduler) {
        this.simulatedCloud = simulatedCloud;
        this.framework = framework;
        this.masterInfo = Protos.MasterInfo.newBuilder()
                .setHostname("titus.embedded")
                .setId("titusEmbedded")
                .setIp(0)
                .setPort(5050)
                .build();
        this.scheduler = scheduler;
        this.driverStatusUpdates.onNext(Status.DRIVER_NOT_STARTED);
    }

    public MesosSchedulerCallbackHandler getMesosSchedulerCallbackHandler() {
        return scheduler;
    }

    public boolean isRunning() {
        return offerUpdateSubscription != null && taskStatusSubscription != null;
    }

    public void addAgent(SimulatedTitusAgent agent) {
        checkIsRunning();

        Subscription offerSubscription = decorateAgentObservable(agent, agent.observeOffers()).subscribe(
                offerChangeEvent -> {
                    offerUpdates.onNext(offerChangeEvent);
                    logger.info("Forwarding offer {} to TitusMaster", offerChangeEvent);
                },
                e -> logger.error("An error emitted in agent {} offer stream", agent.getSlaveId()),
                () -> logger.info("Offer stream from agent {} terminated", agent.getSlaveId())
        );
        Subscription taskUpdateSubscription = decorateAgentObservable(agent, agent.observeTaskUpdates()).subscribe(
                taskStatus -> {
                    taskStatusUpdates.onNext(taskStatus);
                    logger.info("Forwarding task status {} to TitusMaster", PrettyPrinters.printCompact(taskStatus));
                },
                e -> logger.error("An error emitted in agent {} task status stream", agent.getSlaveId()),
                () -> logger.info("Task status stream from agent {} terminated", agent.getSlaveId())
        );

        connectedAgents.put(agent.getSlaveId(), new ConnectedAgent(agent, offerSubscription, taskUpdateSubscription));
    }

    void removeAgent(SimulatedTitusAgent agent) {
        checkIsRunning();

        ConnectedAgent connectedAgent = connectedAgents.get(agent);
        if (connectedAgent != null) {
            connectedAgent.unsubscribeAll();
            logger.info("Removed agent {}", agent.getSlaveId());
        } else {
            logger.info("Remove operation for non-registered agent {}", agent.getSlaveId());
        }
    }

    private void checkIsRunning() {
        Preconditions.checkState(offerUpdateSubscription != null, "Simulated Mesos driver not started by TitusMaster yet");
        Preconditions.checkState(taskStatusSubscription != null, "Simulated Mesos driver not started by TitusMaster yet");
    }

    public Observable<TaskExecutorHolder> observeLaunchedTasks() {
        return launchedTaskUpdates;
    }

    private <T> Observable<T> decorateAgentObservable(SimulatedTitusAgent agent, Observable<T> agentObservable) {
        return agentObservable.doOnTerminate(() -> removeAgent(agent));
    }

    @Override
    public Status start() {
        if (!statusRef.compareAndSet(Status.DRIVER_NOT_STARTED, Status.DRIVER_RUNNING)) {
            throw new IllegalStateException("Mesos driver started twice");
        }
        driverStatusUpdates.onNext(Status.DRIVER_RUNNING);
        scheduler.registered(this, framework.getId(), masterInfo);

        this.offerUpdateSubscription = offerUpdates.subscribe(
                next -> {
                    Protos.Offer offer = next.getOffer();
                    Protos.OfferID offerId = offer.getId();
                    logger.info("Offer update: {}", next);
                    if (next.isRescind()) {
                        scheduler.offerRescinded(this, offerId);
                        // FIXME We have to keep it for some time, otherwise reverse lookup fails
//                        offeredResources.remove(offerId);
                    } else {
                        scheduler.resourceOffers(this, Collections.singletonList(offer));
                        offeredResources.put(offerId, offer);
                    }
                },
                e -> logger.error("Offer updates stream terminated with an error", e),
                () -> logger.error("Offer updates stream onCompleted")
        );
        this.taskStatusSubscription = taskStatusUpdates.subscribe(
                next -> scheduler.statusUpdate(this, next),
                e -> logger.error("Offer updates stream terminated with an error", e),
                () -> logger.error("Offer updates stream onCompleted")
        );

        simulatedCloud.setMesosSchedulerDriver(this);
        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status stop(boolean failover) {
        return stop();
    }

    @Override
    public Status stop() {
        if (statusRef.compareAndSet(Status.DRIVER_RUNNING, Status.DRIVER_STOPPED)) {
            connectedAgents.values().forEach(ca -> removeAgent(ca.getAgent()));

            offerUpdates.onCompleted();
            taskStatusUpdates.onCompleted();

            driverStatusUpdates.onNext(Status.DRIVER_STOPPED);
            driverStatusUpdates.onCompleted();

            logger.info("Mesos driver stopped");
        } else {
            throw new IllegalStateException("Cannot stop mesos driver that is not running");
        }
        return Status.DRIVER_STOPPED;
    }

    @Override
    public Status abort() {
        stop();
        return Status.DRIVER_ABORTED;
    }

    @Override
    public Status join() {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status run() {
        start();
        return driverStatusUpdates.toBlocking().firstOrDefault(null);
    }

    @Override
    public Status requestResources(Collection<Protos.Request> requests) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        checkDriverInRunningState();

        Map<SimulatedTitusAgent, Pair<List<Protos.OfferID>, List<Protos.TaskInfo>>> tasksByAgent = new HashMap<>();
        offerIds.forEach(o -> tasksByAgent.computeIfAbsent(findAgentByOfferId(o), a -> Pair.of(new ArrayList<>(), new ArrayList<>())).getLeft().add(o));
        tasks.forEach(t -> tasksByAgent.computeIfAbsent(findAgentByTaskInfo(t), a -> Pair.of(new ArrayList<>(), new ArrayList<>())).getRight().add(t));

        tasksByAgent.forEach((agent, offersAndTasks) -> {
            List<TaskExecutorHolder> holders = agent.launchTasks(offersAndTasks.getLeft(), offersAndTasks.getRight());
            holders.forEach(holder -> {
                launchedTasks.put(holder.getTaskId(), holder);
                launchedTaskUpdates.onNext(holder);
            });
        });

        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks) {
        return launchTasks(offerIds, tasks, null);
    }

    @Override
    public Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        return launchTasks(Collections.singletonList(offerId), tasks, filters);
    }

    @Override
    public Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks) {
        return launchTasks(Collections.singletonList(offerId), tasks, null);
    }

    @Override
    public Status killTask(Protos.TaskID taskId) {
        TaskExecutorHolder holder = launchedTasks.get(taskId.getValue());
        Protos.SlaveID slaveID = holder == null ? null : holder.getAgent().getSlaveId();
        if (slaveID == null) {
            logger.info("Task {} has not been launched yet; nothing to kill", taskId.getValue());
            taskStatusUpdates.onNext(Protos.TaskStatus.newBuilder()
                    .setState(Protos.TaskState.TASK_LOST)
                    .setTaskId(taskId)
                    .setMessage("Task not found")
                    .build()
            );
            return Status.DRIVER_RUNNING;
        }

        ConnectedAgent connectedAgent = connectedAgents.get(slaveID);
        if (connectedAgent == null) {
            throw new IllegalStateException("Unknown agent id " + slaveID);
        }
        connectedAgent.getAgent().killTask(taskId);

        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status acceptOffers(Collection<Protos.OfferID> offerIds, Collection<Protos.Offer.Operation> operations, Protos.Filters filters) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status declineOffer(Protos.OfferID offerId, Protos.Filters filters) {
        return declineOffer(offerId);
    }

    @Override
    public Status declineOffer(Protos.OfferID offerId) {
        logger.info("Declined offer {}", offerId);

        SimulatedTitusAgent agent = findAgentByOfferId(offerId);
        Preconditions.checkNotNull(agent, "There is no agent who owns offer %s", offerId.getValue());

        agent.declineOffer(offerId);

        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status reviveOffers() {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status suppressOffers() {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status acknowledgeStatusUpdate(Protos.TaskStatus status) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status sendFrameworkMessage(Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status reconcileTasks(Collection<Protos.TaskStatus> statuses) {
        if (statuses.isEmpty()) {
            launchedTasks.values().forEach(h -> scheduler.statusUpdate(this, h.getTaskStatus()));
        } else {
            for (Protos.TaskStatus status : statuses) {
                Optional<TaskExecutorHolder> holder = launchedTasks.values().stream().filter(h -> h.getState() == status.getState()).findFirst();
                if (holder.isPresent()) {
                    scheduler.statusUpdate(this, holder.get().getTaskStatus());
                } else {
                    scheduler.statusUpdate(this, Protos.TaskStatus.newBuilder()
                            .setTaskId(status.getTaskId())
                            .setState(Protos.TaskState.TASK_LOST)
                            .build());
                }
            }
        }
        return Status.DRIVER_RUNNING;
    }

    private void checkDriverInRunningState() {
        Preconditions.checkArgument(statusRef.get() == Status.DRIVER_RUNNING, "driver not in the running state");
    }

    private SimulatedTitusAgent findAgentByOfferId(Protos.OfferID offerID) {
        Protos.Offer firstOffer = offeredResources.get(offerID);
        if (firstOffer == null) {
            throw new IllegalStateException("Unknown offer id " + offerID);
        }

        // All offers are for the same slave.
        Protos.SlaveID slaveId = firstOffer.getSlaveId();
        ConnectedAgent connectedAgent = connectedAgents.get(slaveId);
        if (connectedAgent == null) {
            throw new IllegalStateException("Unknown agent id " + slaveId);
        }
        return connectedAgent.getAgent();
    }

    private SimulatedTitusAgent findAgentByTaskInfo(Protos.TaskInfo taskInfo) {
        ConnectedAgent agent = connectedAgents.get(taskInfo.getSlaveId());
        Preconditions.checkNotNull(agent, "Task " + taskInfo.getTaskId() + " scheduled on unknown agent " + taskInfo.getSlaveId().getValue());
        return agent.getAgent();
    }

    private static class ConnectedAgent {

        private final SimulatedTitusAgent agent;
        private final Subscription offerSubscription;
        private final Subscription taskUpdateSubscription;

        private ConnectedAgent(SimulatedTitusAgent agent, Subscription offerSubscription, Subscription taskUpdateSubscription) {
            this.agent = agent;
            this.offerSubscription = offerSubscription;
            this.taskUpdateSubscription = taskUpdateSubscription;
        }

        private void unsubscribeAll() {
            offerSubscription.unsubscribe();
            taskUpdateSubscription.unsubscribe();
        }

        public SimulatedTitusAgent getAgent() {
            return agent;
        }
    }
}
