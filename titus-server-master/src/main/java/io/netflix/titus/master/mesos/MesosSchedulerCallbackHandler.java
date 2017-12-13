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
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import io.netflix.titus.master.MetricConstants;
import io.netflix.titus.master.Status;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes;
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

import static io.netflix.titus.master.mesos.MesosTracer.logMesosCallbackDebug;
import static io.netflix.titus.master.mesos.MesosTracer.logMesosCallbackError;
import static io.netflix.titus.master.mesos.MesosTracer.logMesosCallbackInfo;
import static io.netflix.titus.master.mesos.MesosTracer.logMesosCallbackWarn;
import static io.netflix.titus.master.mesos.MesosTracer.traceMesosRequest;

public class MesosSchedulerCallbackHandler implements Scheduler {

    private Observer<String> vmLeaseRescindedObserver;
    private Observer<Status> vmTaskStatusObserver;
    private static final Logger logger = LoggerFactory.getLogger(MesosSchedulerCallbackHandler.class);
    private final V2JobOperations v2JobOperations;
    private final V3JobOperations v3JobOperations;
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
            Action1<List<? extends VirtualMachineLease>> leaseHandler,
            Observer<String> vmLeaseRescindedObserver,
            Observer<Status> vmTaskStatusObserver,
            V2JobOperations v2JobOperations,
            V3JobOperations v3JobOperations,
            MasterConfiguration config,
            Registry registry) {
        this.leaseHandler = leaseHandler;
        this.vmLeaseRescindedObserver = vmLeaseRescindedObserver;
        this.vmTaskStatusObserver = vmTaskStatusObserver;
        this.v2JobOperations = v2JobOperations;
        this.v3JobOperations = v3JobOperations;
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
                    logMesosCallbackInfo("Declining offer: %s due to too few CPUs in offer from %s: %s", offer.getId().getValue(), offer.getHostname(), cpus);
                    return false;
                }
            } else if ("mem".equals(resource.getName())) {
                double memoryMB = resource.getScalar().getValue();
                if (memoryMB < 1) {
                    logMesosCallbackInfo("Declining offer: %s due to too few memory in offer from %s: %s", offer.getId().getValue(), offer.getHostname(), memoryMB);
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
                        traceMesosRequest(
                                "Declining new offer: " + offer.getId(),
                                () -> driver.declineOffer(offer.getId(), (Protos.Filters.getDefaultInstance().toBuilder()).setRefuseSeconds(60).build())
                        );
                        numOfferTooSmall.increment();
                        return false;
                    }
                    logMesosCallbackInfo("Adding offer: " + offer.getId().getValue() + " from host: " + offer.getHostname());
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
    public void disconnected(SchedulerDriver driver) {
        logMesosCallbackError("Mesos driver disconnected: %s", driver);
        numMesosDisconnects.increment();
        connected = false;
    }

    @Override
    public void error(SchedulerDriver driver, String msg) {
        logMesosCallbackError("Error from Mesos: %s", msg);
    }

    @Override
    public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) {
        logMesosCallbackError("Lost executor %s on slave %s with status=%s", executorId.getValue(), slaveId.getValue(), status);
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
        logMesosCallbackError("Unexpected framework message: executorId=%s slaveID=%s, message=%s", executorId.getValue(), slaveId.getValue(), data);
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
        logMesosCallbackInfo("Rescinded offer: %s", offerId.getValue());
        vmLeaseRescindedObserver.onNext(offerId.getValue());
        numOfferRescinded.increment();
    }

    @Override
    public void registered(SchedulerDriver driver, FrameworkID frameworkID, MasterInfo masterInfo) {
        logMesosCallbackInfo("Mesos registered: %s, ID=%s, masterInfo=%s", driver, frameworkID.getValue(), masterInfo.getId());
        initializeNewDriver(driver);
        numMesosRegistered.increment();
        connected = true;
    }

    @Override
    public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
        logMesosCallbackWarn("Mesos re-registered: %s, masterInfo=%s", driver, masterInfo.getId());
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

        List<V2WorkerMetadata> runningWorkers = new ArrayList<>();
        v2JobOperations.getAllJobMgrs().forEach(m -> {
                    List<V2WorkerMetadata> tasks = m.getWorkers().stream()
                            .filter(t -> V2JobState.isRunningState(t.getState()))
                            .collect(Collectors.toList());
                    runningWorkers.addAll(tasks);
                }
        );
        for (V2WorkerMetadata mwmd : runningWorkers) {
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
        for (Task task : v3JobOperations.getTasks()) {
            io.netflix.titus.api.jobmanager.model.job.TaskState taskState = task.getStatus().getState();
            if (io.netflix.titus.api.jobmanager.model.job.TaskState.isRunning(taskState)) {
                String taskHost = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST);
                if (taskHost != null) {
                    tasksToInitialize.add(TaskStatus.newBuilder()
                            .setTaskId(Protos.TaskID.newBuilder().setValue(task.getId()).build())
                            .setState(TaskState.TASK_RUNNING)
                            .setSlaveId(SlaveID.newBuilder().setValue(taskHost).build())
                            .build()
                    );
                }
            }
        }
        if (!tasksToInitialize.isEmpty()) {
            Protos.Status status = traceMesosRequest(
                    "Reconciling active tasks: count=" + tasksToInitialize.size(),
                    () -> driver.reconcileTasks(tasksToInitialize)
            );
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
        Protos.Status status = traceMesosRequest(
                "Reconciling all active tasks",
                () -> driver.reconcileTasks(Collections.emptyList())
        );
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
    public void slaveLost(SchedulerDriver arg0, SlaveID slaveId) {
        logMesosCallbackWarn("Lost slave: %s", slaveId.getValue());
    }

    @Override
    public void statusUpdate(final SchedulerDriver arg0, TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();
        TaskState taskState = taskStatus.getState();

        logMesosCallbackInfo("Task status update: taskId=%s, taskState=%s, message=%s", taskId, taskState, taskStatus.getMessage());

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
        String data = "";
        if (taskStatus.getData() != null) {
            data = new String(taskStatus.getData().toByteArray());
            logMesosCallbackDebug("Mesos status object data: %s", data);
        }
        WorkerNaming.JobWorkerIdPair pair = WorkerNaming.getJobAndWorkerId(taskId);
        final Status status = new Status(pair.jobId, taskId, -1, pair.workerIndex, pair.workerNumber, Status.TYPE.ERROR,
                taskStatus.getMessage(), data, state);
        status.setReason(reason);

        logger.debug("Publishing task status: {}", status);

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
