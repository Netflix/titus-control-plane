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

package io.netflix.titus.testkit.embedded.cloud.connector.local;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.connector.ConnectorUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;

/**
 * A stub implementation of {@link SchedulerDriver} to simulate Mesos framework and a collection of agent/resources.
 * The implementation is minimalistic, and covers only features used by Titus master.
 */
public class SimulatedLocalMesosSchedulerDriver implements SchedulerDriver {

    private static final Logger logger = LoggerFactory.getLogger(SimulatedLocalMesosSchedulerDriver.class);

    private final SimulatedCloud simulatedCloud;

    private final Protos.FrameworkInfo framework;
    private final Protos.MasterInfo masterInfo;

    private final Scheduler scheduler;

    private Subscription offerUpdateSubscription;
    private Subscription taskStatusSubscription;

    private final AtomicReference<Status> statusRef = new AtomicReference<>(Status.DRIVER_NOT_STARTED);

    public SimulatedLocalMesosSchedulerDriver(SimulatedCloud simulatedCloud,
                                              Protos.FrameworkInfo framework,
                                              Scheduler scheduler) {
        this.simulatedCloud = simulatedCloud;
        this.framework = framework;
        this.masterInfo = Protos.MasterInfo.newBuilder()
                .setHostname("titus.embedded")
                .setId("titusEmbedded")
                .setIp(0)
                .setPort(5050)
                .build();
        this.scheduler = scheduler;
    }

    public Scheduler getMesosSchedulerCallbackHandler() {
        return scheduler;
    }

    public boolean isRunning() {
        return statusRef.get() == Status.DRIVER_RUNNING;
    }

    public Observable<TaskExecutorHolder> observeLaunchedTasks() {
        return simulatedCloud.taskLaunches();
    }

    @Override
    public Status start() {
        if (!statusRef.compareAndSet(Status.DRIVER_NOT_STARTED, Status.DRIVER_RUNNING)) {
            throw new IllegalStateException("Mesos driver started twice");
        }
        scheduler.registered(this, framework.getId(), masterInfo);

        this.offerUpdateSubscription = simulatedCloud.offers().subscribe(
                next -> {
                    Protos.Offer offer = next.getOffer();
                    Protos.OfferID offerId = offer.getId();
                    logger.info("Offer update: {}", next);
                    if (next.isRescind()) {
                        scheduler.offerRescinded(this, offerId);
                    } else {
                        scheduler.resourceOffers(this, Collections.singletonList(offer));
                    }
                },
                e -> logger.error("Offer updates stream terminated with an error", e),
                () -> logger.error("Offer updates stream onCompleted")
        );
        this.taskStatusSubscription = simulatedCloud.taskStatusUpdates().subscribe(
                next -> {
                    logger.info("Task status update from Mesos: {}", next);
                    scheduler.statusUpdate(this, next);
                },
                e -> logger.error("Offer updates stream terminated with an error", e),
                () -> logger.error("Offer updates stream onCompleted")
        );

        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status stop(boolean failover) {
        return stop();
    }

    @Override
    public Status stop() {
        if (statusRef.compareAndSet(Status.DRIVER_RUNNING, Status.DRIVER_STOPPED)) {

            ObservableExt.safeUnsubscribe(offerUpdateSubscription, taskStatusSubscription);

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
        return Status.DRIVER_RUNNING;
    }

    @Override
    public Status requestResources(Collection<Protos.Request> requests) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        checkDriverInRunningState();
        simulatedCloud.launchTasks(ConnectorUtils.findEarliestLease(offerIds), tasks);
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
        checkDriverInRunningState();
        simulatedCloud.killTask(taskId.getValue());
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
        checkDriverInRunningState();
        simulatedCloud.declineOffer(offerId.getValue());
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
        checkDriverInRunningState();
        if (statuses.isEmpty()) {
            simulatedCloud.reconcileKnownTasks();
        } else {
            simulatedCloud.reconcileTasks(statuses.stream().map(s -> s.getTaskId().getValue()).collect(Collectors.toSet()));
        }
        return Status.DRIVER_RUNNING;
    }

    private void checkDriverInRunningState() {
        Preconditions.checkArgument(statusRef.get() == Status.DRIVER_RUNNING, "driver not in the running state");
    }
}
