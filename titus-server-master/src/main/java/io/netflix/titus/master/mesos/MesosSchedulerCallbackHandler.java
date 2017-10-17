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

package io.netflix.titus.master.mesos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.config.MasterConfiguration;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Func0;

public class MesosSchedulerCallbackHandler implements Scheduler {

    private Observer<String> vmLeaseRescindedObserver;
    private Observer<Status> vmTaskStatusObserver;
    private static final Logger logger = LoggerFactory.getLogger(MesosSchedulerCallbackHandler.class);
    private Func0<List<V2WorkerMetadata>> runningWorkersGetter;
    private volatile ScheduledFuture reconcilerFuture = null;
    private final MasterConfiguration config;
    private AtomicLong lastOfferReceivedAt = new AtomicLong(System.currentTimeMillis());
    private AtomicLong lastValidOfferReceivedAt = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastOfferReceivedMillis;
    private final AtomicLong lastValidOfferReceiveMillis;
    private final Counter numMesosRegistered;
    private final Counter numMesosDisconnects;
    private final Counter numOfferRescinded;
    private final Counter numReconcileTasks;
    private final Counter numInvalidOffers;
    private final Counter numOfferTooSmall;
    private long reconciliationTrial = 0;
    private final com.netflix.fenzo.functions.Action1<List<? extends VirtualMachineLease>> leaseHandler;

    private final Subscription subscription;
    private ScheduledThreadPoolExecutor executor;
    private boolean connected;

    /**
     * Due to race condition in the initialization process, we may miss some state updates. For missed final updates
     * the reconciliation process fails, and results in tasks in 'CRASHED' state. By caching last state update
     * we can detect such scenarios, and return the last known final state, instead of 'TASK_LOST'.
     */
    private final Cache<String, TaskState> lastStatusUpdate = CacheBuilder.newBuilder()
            .expireAfterWrite(10, TimeUnit.MINUTES)
            .build();

    public MesosSchedulerCallbackHandler(
            com.netflix.fenzo.functions.Action1<List<? extends VirtualMachineLease>> leaseHandler,
            Observer<String> vmLeaseRescindedObserver,
            Observer<Status> vmTaskStatusObserver,
            Func0<List<V2WorkerMetadata>> runningWorkersGetter,
            MasterConfiguration config,
            Registry registry) {
        this.leaseHandler = leaseHandler;
        this.vmLeaseRescindedObserver = vmLeaseRescindedObserver;
        this.vmTaskStatusObserver = vmTaskStatusObserver;
        this.runningWorkersGetter = runningWorkersGetter;
        this.config = config;
        numMesosRegistered = registry.counter(MetricConstants.METRIC_MESOS + "numMesosRegistered");
        numMesosDisconnects = registry.counter(MetricConstants.METRIC_MESOS + "numMesosDisconnects");
        numOfferRescinded = registry.counter(MetricConstants.METRIC_MESOS + "numOfferRescinded");
        numReconcileTasks = registry.counter(MetricConstants.METRIC_MESOS + "numReconcileTasks");
        lastOfferReceivedMillis = registry.gauge(MetricConstants.METRIC_MESOS + "lastOfferReceivedMillis", new AtomicLong());
        lastValidOfferReceiveMillis = registry.gauge(MetricConstants.METRIC_MESOS + "lastValidOfferReceiveMillis", new AtomicLong());
        numInvalidOffers = registry.counter(MetricConstants.METRIC_MESOS + "numInvalidOffers");
        numOfferTooSmall = registry.counter(MetricConstants.METRIC_MESOS + "numOfferTooSmall");
        this.subscription = Observable
                .interval(10, 10, TimeUnit.SECONDS)
                .doOnNext(tick -> {
                    lastOfferReceivedMillis.set(System.currentTimeMillis() - lastOfferReceivedAt.get());
                    lastValidOfferReceiveMillis.set(System.currentTimeMillis() - lastValidOfferReceivedAt.get());
                })
                .subscribe();
    }

    public void shutdown() {
        try {
            if (executor != null) {
                executor.shutdown();
            }
            subscription.unsubscribe();
        } finally {
            connected = false;
        }
    }

    // simple offer resource validator
    private boolean validateOfferResources(Offer offer) {
        for (Protos.Resource resource : offer.getResourcesList()) {
            if ("cpus".equals(resource.getName())) {
                final double cpus = resource.getScalar().getValue();
                if (cpus < 0.1) {
                    logger.warn("Declining offer: " + offer.getId().getValue() + " due to too few CPUs in offer from " + offer.getHostname() +
                            ": " + cpus);
                    return false;
                }
            } else if ("mem".equals(resource.getName())) {
                double memoryMB = resource.getScalar().getValue();
                if (memoryMB < 1) {
                    logger.warn("Declining offer: " + offer.getId().getValue() + " due to too few memory in offer from " + offer.getHostname() +
                            ": " + memoryMB);
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
        lastOfferReceivedAt.set(System.currentTimeMillis());
        final List<VMLeaseObject> leaseObjects = offers.stream()
                .filter(offer -> {
                    if (!validateOfferResources(offer)) {
                        driver.declineOffer(offer.getId(), (Protos.Filters.getDefaultInstance().toBuilder()).setRefuseSeconds(60).build());
                        numOfferTooSmall.increment();
                        return false;
                    }
                    logger.info("Adding offer: " + offer.getId().getValue() + " from host: " + offer.getHostname());
                    return true;
                })
                .map(VMLeaseObject::new)
                .collect(Collectors.toList());
        if (leaseObjects != null && !leaseObjects.isEmpty()) {
            lastValidOfferReceivedAt.set(System.currentTimeMillis());
            if (offers.size() > leaseObjects.size()) {
                numInvalidOffers.increment(offers.size() - leaseObjects.size());
            }
        }
        leaseHandler.call(leaseObjects);
    }

    @Override
    public void disconnected(SchedulerDriver arg0) {
        logger.warn("Mesos driver disconnected: " + arg0);
        numMesosDisconnects.increment();
        connected = false;
    }

    @Override
    public void error(SchedulerDriver arg0, String msg) {
        logger.error("Error from Mesos: " + msg);
    }

    @Override
    public void executorLost(SchedulerDriver arg0, ExecutorID arg1,
                             SlaveID arg2, int arg3) {
        logger.warn("Lost executor " + arg1.getValue() + " on slave " + arg2.getValue() + " with status=" + arg3);
    }

    @Override
    public void frameworkMessage(SchedulerDriver arg0, ExecutorID arg1,
                                 SlaveID arg2, byte[] arg3) {
        logger.warn("Unexpected framework message: executorId=" + arg1.getValue() +
                ", slaveID=" + arg2.getValue() + ", message=" + arg3);
    }

    @Override
    public void offerRescinded(SchedulerDriver arg0, OfferID arg1) {
        logger.warn("Rescinded offer: " + arg1.getValue());
        vmLeaseRescindedObserver.onNext(arg1.getValue());
        numOfferRescinded.increment();
    }

    @Override
    public void registered(SchedulerDriver driver, FrameworkID frameworkID,
                           MasterInfo masterInfo) {
        logger.info("Mesos registered: " + driver + ", ID=" + frameworkID.getValue() + ", masterInfo=" + masterInfo.getId());
        initializeNewDriver(driver);
        numMesosRegistered.increment();
        connected = true;
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo arg1) {
        logger.info("Mesos re-registered: " + driver + ", masterInfo=" + arg1.getId());
        initializeNewDriver(driver);
        numMesosRegistered.increment();
        connected = true;
    }

    boolean isConnected() {
        return connected;
    }

    private synchronized void initializeNewDriver(final SchedulerDriver driver) {
        vmLeaseRescindedObserver.onNext("ALL");
        if (reconcilerFuture != null) {
            reconcilerFuture.cancel(true);
        }
        this.executor = new ScheduledThreadPoolExecutor(1);
        reconcilerFuture = executor.scheduleWithFixedDelay(() -> reconcileTasks(driver), 30, config.getMesosTaskReconciliationIntervalSecs(), TimeUnit.SECONDS);
    }

    public void reconcileTasks(final SchedulerDriver driver) {
        try {
            if (reconciliationTrial++ % 2 == 0) {
                reconcileTasksKnownToUs(driver);
            } else {
                reconcileAllMesosTasks(driver);
            }
        } catch (Exception e) {
            // we don't want to throw errors lest periodically scheduled reconciliation be cancelled
            logger.error("Unexpected error (continuing): " + e.getMessage(), e);
        }
    }

    private void reconcileTasksKnownToUs(SchedulerDriver driver) {
        final List<TaskStatus> tasksToInitialize = new ArrayList<>();
        for (V2WorkerMetadata mwmd : runningWorkersGetter.call()) {
            tasksToInitialize.add(TaskStatus.newBuilder()
                    .setTaskId(
                            Protos.TaskID.newBuilder()
                                    .setValue(
                                            WorkerNaming.getWorkerName(
                                                    mwmd.getJobId(),
                                                    mwmd.getWorkerIndex(),
                                                    mwmd.getWorkerNumber()))
                                    .build())
                    .setState(TaskState.TASK_RUNNING)
                    .setSlaveId(SlaveID.newBuilder().setValue(mwmd.getSlaveID()).build())
                    .build()
            );
        }
        if (!tasksToInitialize.isEmpty()) {
            Protos.Status status = driver.reconcileTasks(tasksToInitialize);
            numReconcileTasks.increment();
            logger.info("Sent request to reconcile " + tasksToInitialize.size() + " tasks, status=" + status);
            logger.info("Last offer received " + (System.currentTimeMillis() - lastOfferReceivedAt.get()) / 1000 + " secs ago");
            logger.info("Last valid offer received " + (System.currentTimeMillis() - lastValidOfferReceivedAt.get()) / 1000 + " secs ago");
            switch (status) {
                case DRIVER_ABORTED:
                case DRIVER_STOPPED:
                    logger.error("Unexpected to see Mesos driver status of " + status + " from reconcile request. Committing suicide!");
                    System.exit(2);
            }
        }
    }

    private void reconcileAllMesosTasks(SchedulerDriver driver) {
        Protos.Status status = driver.reconcileTasks(Collections.emptyList());
        numReconcileTasks.increment();
        logger.info("Sent request to reconcile all tasks known to Mesos");
        logger.info("Last offer received " + (System.currentTimeMillis() - lastOfferReceivedAt.get()) / 1000 + " secs ago");
        logger.info("Last valid offer received " + (System.currentTimeMillis() - lastValidOfferReceivedAt.get()) / 1000 + " secs ago");
        switch (status) {
            case DRIVER_ABORTED:
            case DRIVER_STOPPED:
                logger.error("Unexpected to see Mesos driver status of " + status + " from reconcile request (all tasks). Committing suicide!");
                System.exit(2);
        }
    }

    @Override
    public void slaveLost(SchedulerDriver arg0, SlaveID arg1) {
        logger.warn("Lost slave " + arg1.getValue());
    }

    @Override
    public void statusUpdate(final SchedulerDriver arg0, TaskStatus arg1) {
        String taskId = arg1.getTaskId().getValue();
        TaskState taskState = arg1.getState();

        WorkerNaming.JobWorkerIdPair pair = WorkerNaming.getJobAndWorkerId(taskId);
        logger.info("Task status update: (" + taskId + ")" + pair.jobId +
                " worker index " + pair.workerIndex + " number " + pair.workerNumber + ": " + taskState + " (" +
                taskState.getNumber() + ") - " + arg1.getMessage());

        TaskState previous = lastStatusUpdate.getIfPresent(taskId);
        TaskState effectiveState;
        if (previous != null && isTerminal(previous) && taskState == TaskState.TASK_LOST) {
            effectiveState = previous;
            // Replace task status only once (as we cannot remove item from cache, we overwrite the value)
            lastStatusUpdate.put(taskId, taskState);
        } else {
            effectiveState = taskState;
            lastStatusUpdate.put(taskId, taskState);
        }

        V2JobState state;
        JobCompletedReason reason = JobCompletedReason.Normal;
        switch (effectiveState) {
            case TASK_FAILED:
                state = V2JobState.Failed;
                reason = JobCompletedReason.Failed;
                break;
            case TASK_LOST:
                state = V2JobState.Failed;
                reason = JobCompletedReason.Lost;
                break;
            case TASK_KILLED:
                state = V2JobState.Failed;
                reason = JobCompletedReason.Killed;
                break;
            case TASK_FINISHED:
                state = V2JobState.Completed;
                break;
            case TASK_RUNNING:
                state = V2JobState.Started;
                break;
            case TASK_STAGING:
                state = V2JobState.Launched;
                break;
            case TASK_STARTING:
                state = V2JobState.StartInitiated;
                break;
            default:
                logger.warn("Unexpected Mesos task state " + effectiveState);
                return;
        }
        if (arg1.getData() != null) {
            logger.info("Mesos status object data: " + new String(arg1.getData().toByteArray()));
        }
        final Status status = new Status(pair.jobId, taskId, -1, pair.workerIndex, pair.workerNumber, Status.TYPE.ERROR,
                arg1.getMessage(), arg1.getData() == null ? "" : new String(arg1.getData().toByteArray()), state);
        status.setReason(reason);

        logger.info("Publishing task status {}", status);

        vmTaskStatusObserver.onNext(status);
    }

    private boolean isTerminal(TaskState taskState) {
        return taskState == TaskState.TASK_FINISHED
                || taskState == TaskState.TASK_ERROR
                || taskState == TaskState.TASK_FAILED
                || taskState == TaskState.TASK_LOST
                || taskState == TaskState.TASK_KILLED;
    }
}
