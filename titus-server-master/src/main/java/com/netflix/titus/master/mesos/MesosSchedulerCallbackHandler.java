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

package com.netflix.titus.master.mesos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.RegExpExt;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.SystemExt;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.jobmanager.service.JobManagerUtil;
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
import rx.schedulers.Schedulers;

import static com.netflix.titus.master.mesos.MesosTracer.logMesosCallbackDebug;
import static com.netflix.titus.master.mesos.MesosTracer.logMesosCallbackError;
import static com.netflix.titus.master.mesos.MesosTracer.logMesosCallbackInfo;
import static com.netflix.titus.master.mesos.MesosTracer.logMesosCallbackWarn;
import static com.netflix.titus.master.mesos.MesosTracer.traceMesosRequest;

public class MesosSchedulerCallbackHandler implements Scheduler {

    private static final Set<TaskState> ACTIVE_MESOS_TASK_STATES = CollectionsExt.asSet(
            TaskState.TASK_STAGING,
            TaskState.TASK_STARTING,
            TaskState.TASK_RUNNING
    );

    private Observer<LeaseRescindedEvent> vmLeaseRescindedObserver;
    private Observer<ContainerEvent> vmTaskStatusObserver;
    private static final Logger logger = LoggerFactory.getLogger(MesosSchedulerCallbackHandler.class);
    private final V3JobOperations v3JobOperations;
    private volatile ScheduledFuture reconcilerFuture = null;
    private final MasterConfiguration config;
    private final MesosConfiguration mesosConfiguration;
    private final Registry registry;
    private final Optional<FitInjection> taskStatusUpdateFitInjection;
    private final MesosStateTracker mesosStateTracker;

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

    private final Function<String, Matcher> invalidRequestMessageMatcherFactory;
    private final Function<String, Matcher> crashedMessageMatcherFactory;
    private final Function<String, Matcher> transientSystemErrorMessageMatcherFactory;
    private final Function<String, Matcher> localSystemErrorMessageMatcherFactory;
    private final Function<String, Matcher> unknownSystemErrorMessageMatcherFactory;

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
            Observer<LeaseRescindedEvent> vmLeaseRescindedObserver,
            Observer<ContainerEvent> vmTaskStatusObserver,
            V3JobOperations v3JobOperations,
            Optional<FitInjection> taskStatusUpdateFitInjection,
            MasterConfiguration config,
            MesosConfiguration mesosConfiguration,
            TitusRuntime titusRuntime) {
        this.leaseHandler = leaseHandler;
        this.vmLeaseRescindedObserver = vmLeaseRescindedObserver;
        this.vmTaskStatusObserver = vmTaskStatusObserver;
        this.v3JobOperations = v3JobOperations;
        this.taskStatusUpdateFitInjection = taskStatusUpdateFitInjection;
        this.config = config;
        this.mesosConfiguration = mesosConfiguration;
        this.registry = titusRuntime.getRegistry();
        this.mesosStateTracker = new MesosStateTracker(config, titusRuntime, Schedulers.computation());

        numMesosRegistered = registry.counter(MetricConstants.METRIC_MESOS + "numMesosRegistered");
        numMesosDisconnects = registry.counter(MetricConstants.METRIC_MESOS + "numMesosDisconnects");
        numOfferRescinded = registry.counter(MetricConstants.METRIC_MESOS + "numOfferRescinded");
        numReconcileTasks = registry.counter(MetricConstants.METRIC_MESOS + "numReconcileTasks");
        lastOfferReceivedMillis = registry.gauge(MetricConstants.METRIC_MESOS + "lastOfferReceivedMillis", new AtomicLong());
        lastValidOfferReceiveMillis = registry.gauge(MetricConstants.METRIC_MESOS + "lastValidOfferReceiveMillis", new AtomicLong());
        numInvalidOffers = registry.counter(MetricConstants.METRIC_MESOS + "numInvalidOffers");
        numOfferTooSmall = registry.counter(MetricConstants.METRIC_MESOS + "numOfferTooSmall");

        this.invalidRequestMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getInvalidRequestMessagePattern, "invalidRequestMessagePattern", Pattern.DOTALL, logger);
        this.crashedMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getCrashedMessagePattern, "crashedMessagePattern", Pattern.DOTALL, logger);
        this.transientSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getTransientSystemErrorMessagePattern, "transientSystemErrorMessagePattern", Pattern.DOTALL, logger);
        this.localSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getLocalSystemErrorMessagePattern, "localSystemErrorMessagePattern", Pattern.DOTALL, logger);
        this.unknownSystemErrorMessageMatcherFactory = RegExpExt.dynamicMatcher(mesosConfiguration::getUnknownSystemErrorMessagePattern, "unknownSystemErrorMessagePattern", Pattern.DOTALL, logger);

        this.subscription = Observable
                .interval(10, 10, TimeUnit.SECONDS)
                .doOnNext(tick -> {
                    lastOfferReceivedMillis.set(System.currentTimeMillis() - lastOfferReceivedAt.get());
                    lastValidOfferReceiveMillis.set(System.currentTimeMillis() - lastValidOfferReceivedAt.get());
                })
                .subscribe();
    }

    public void shutdown() {
        mesosStateTracker.shutdown();
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
        if (!leaseObjects.isEmpty()) {
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
        String leaseId = offerId.getValue();
        logMesosCallbackInfo("Rescinded offer: %s", leaseId);
        vmLeaseRescindedObserver.onNext(LeaseRescindedEvent.leaseIdEvent(leaseId));
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
        vmLeaseRescindedObserver.onNext(LeaseRescindedEvent.allEvent());
        if (reconcilerFuture != null) {
            reconcilerFuture.cancel(true);
        }
        this.executor = new ScheduledThreadPoolExecutor(1);
        reconcilerFuture = executor.scheduleWithFixedDelay(() -> reconcileTasks(driver), 30, config.getMesosTaskReconciliationIntervalSecs(), TimeUnit.SECONDS);
    }

    public void reconcileTasks(final SchedulerDriver driver) {
        if (!mesosConfiguration.isReconcilerEnabled()) {
            logger.info("Task reconciliation is turned-off");
            return;
        }
        try {
            if (reconciliationTrial++ % 2 == 0) {
                reconcileTasksKnownToUs(driver);
            } else {
                reconcileAllMesosTasks(driver);
            }
        } catch (Exception e) {
            // we don't want to throw errors lest periodically scheduled reconciliation be cancelled
            logger.error("Unexpected error (continuing): {}", e.getMessage(), e);
        }
    }

    private void reconcileTasksKnownToUs(SchedulerDriver driver) {
        final List<TaskStatus> tasksToInitialize = new ArrayList<>();

        for (Task task : v3JobOperations.getTasks()) {
            com.netflix.titus.api.jobmanager.model.job.TaskState taskState = task.getStatus().getState();
            TaskState mesosState;
            switch (taskState) {
                case Started:
                    mesosState = TaskState.TASK_RUNNING;
                    break;
                case KillInitiated:
                    mesosState = TaskState.TASK_KILLING;
                    break;
                default:
                    mesosState = null;
            }
            if (mesosState != null) {
                String taskHost = task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST);
                if (taskHost != null) {
                    tasksToInitialize.add(TaskStatus.newBuilder()
                            .setTaskId(Protos.TaskID.newBuilder().setValue(task.getId()).build())
                            .setState(mesosState)
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
            logger.info("Sent request to reconcile {} tasks, status={}", tasksToInitialize.size(), status);
            logger.info("Last offer received {} secs ago", (System.currentTimeMillis() - lastOfferReceivedAt.get()) / 1000);
            logger.info("Last valid offer received {} secs ago", (System.currentTimeMillis() - lastValidOfferReceivedAt.get()) / 1000);
            switch (status) {
                case DRIVER_ABORTED:
                case DRIVER_STOPPED:
                    logger.error("Unexpected to see Mesos driver status of {} from reconcile request. Committing suicide!", status);
                    SystemExt.forcedProcessExit(2);
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
        logger.info("Last offer received {} secs ago", (System.currentTimeMillis() - lastOfferReceivedAt.get()) / 1000);
        logger.info("Last valid offer received {} secs ago", (System.currentTimeMillis() - lastValidOfferReceivedAt.get()) / 1000);
        switch (status) {
            case DRIVER_ABORTED:
            case DRIVER_STOPPED:
                logger.error("Unexpected to see Mesos driver status of {} from reconcile request (all tasks). Committing suicide!", status);
                SystemExt.forcedProcessExit(2);
        }
    }

    @Override
    public void slaveLost(SchedulerDriver arg0, SlaveID slaveId) {
        logMesosCallbackWarn("Lost slave: %s", slaveId.getValue());
    }

    @Override
    public void statusUpdate(final SchedulerDriver arg0, TaskStatus taskStatus) {
        try {
            String taskId = taskStatus.getTaskId().getValue();
            TaskState taskState = taskStatus.getState();

            TaskStatus effectiveTaskStatus = taskStatusUpdateFitInjection.map(i -> i.afterImmediate("update", taskStatus)).orElse(taskStatus);

            if (isReconcilerUpdateForUnknownTask(effectiveTaskStatus)) {
                if (taskStatus.getState() == TaskState.TASK_LOST) {
                    logger.info("Ignoring reconciler TASK_LOST status update for task: {}", taskId);
                    return;
                }
                mesosStateTracker.unknownTaskStatusUpdate(taskStatus);
                if (!mesosConfiguration.isAllowReconcilerUpdatesForUnknownTasks()) {
                    logger.info("Ignoring reconciler triggered task status update: {}", taskId);
                    return;
                }
            } else {
                mesosStateTracker.knownTaskStatusUpdate(taskStatus);
            }

            logMesosCallbackInfo("Task status update: taskId=%s, taskState=%s, message=%s", taskId, taskState, effectiveTaskStatus.getMessage());

            v3StatusUpdate(effectiveTaskStatus);
        } catch (Exception e) {
            logger.error("Unexpected error when handling the status update: {}", taskStatus, e);
            throw e;
        }
    }

    private boolean isReconcilerUpdateForUnknownTask(TaskStatus taskStatus) {
        if (taskStatus.getReason() != TaskStatus.Reason.REASON_RECONCILIATION) {
            return false;
        }

        String taskId = taskStatus.getTaskId().getValue();
        return !isKnown(taskId);
    }

    private boolean isKnown(String taskId) {
        return v3JobOperations.findTaskById(taskId).isPresent();
    }

    private void v3StatusUpdate(TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();
        TaskState taskState = taskStatus.getState();

        TaskState previous = lastStatusUpdate.getIfPresent(taskId);
        TaskState effectiveState = getEffectiveState(taskId, taskState, previous);

        com.netflix.titus.api.jobmanager.model.job.TaskState v3TaskState;
        String reasonCode;

        /*
         * Some of Mesos states could be mapped here directly to Titus system errors. We do not do that, and instead
         * we depend on the error message pattern matching to isolate system-level errors. Tasks failed due to system errors
         * are always retried, and so we want to have a full control over error conditions when we do that.
         */
        switch (effectiveState) {
            case TASK_STAGING:
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Launched;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
                break;
            case TASK_STARTING:
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.StartInitiated;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
                break;
            case TASK_RUNNING:
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Started;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
                break;
            case TASK_ERROR: // TERMINAL: The task description contains an error.
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_INVALID_REQUEST;
                break;
            case TASK_FAILED: // TERMINAL: The task failed to finish successfully.
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
                break;
            case TASK_LOST: // The task failed but can be rescheduled.
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;
                break;
            case TASK_UNKNOWN: // The master has no knowledge of the task.
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;
                break;
            case TASK_KILLED: // TERMINAL: The task was killed by the executor.
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_KILLED;
                break;
            case TASK_KILLING:
                // Ignore today. In the future we can split Titus KillInitiated state into two steps: KillRequested and Killing.
                return;
            case TASK_FINISHED: // The task finished successfully on its own without external interference.
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_NORMAL;
                break;
            case TASK_DROPPED: // The task failed to launch because of a transient error.
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_FAILED;
                break;
            case TASK_UNREACHABLE: // The task was running on an agent that has lost contact with the master
                // Ignore. We will handle this state once we add 'Disconnected' state support in Titus.
                return;
            case TASK_GONE: // The task is no longer running. This can occur if the agent has been terminated along with all of its tasks
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;
                break;
            case TASK_GONE_BY_OPERATOR: // The task was running on an agent that the master cannot contact; the operator has asserted that the agent has been shutdown
                v3TaskState = com.netflix.titus.api.jobmanager.model.job.TaskState.Finished;
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TASK_LOST;
                break;
            default:
                logger.warn("Unexpected Mesos task state {}", effectiveState);
                return;
        }

        if (v3TaskState == com.netflix.titus.api.jobmanager.model.job.TaskState.Finished && !StringExt.isEmpty(taskStatus.getMessage())) {
            String message = taskStatus.getMessage();
            if (invalidRequestMessageMatcherFactory.apply(message).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_INVALID_REQUEST;
            } else if (crashedMessageMatcherFactory.apply(message).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_CRASHED;
            } else if (transientSystemErrorMessageMatcherFactory.apply(message).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_TRANSIENT_SYSTEM_ERROR;
            } else if (localSystemErrorMessageMatcherFactory.apply(message).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_LOCAL_SYSTEM_ERROR;
            } else if (unknownSystemErrorMessageMatcherFactory.apply(message).matches()) {
                reasonCode = com.netflix.titus.api.jobmanager.model.job.TaskStatus.REASON_UNKNOWN_SYSTEM_ERROR;
            }
        }

        Optional<TitusExecutorDetails> details;
        if (taskStatus.getData() != null) {
            String data = new String(taskStatus.getData().toByteArray());
            logMesosCallbackDebug("Mesos status object data: %s", data);
            details = JobManagerUtil.parseDetails(data);
        } else {
            details = Optional.empty();
        }

        V3ContainerEvent event = new V3ContainerEvent(
                taskId,
                v3TaskState,
                reasonCode,
                taskStatus.getMessage(),
                System.currentTimeMillis(),
                details
        );

        logger.debug("Publishing task status: {}", event);
        vmTaskStatusObserver.onNext(event);
    }

    private TaskState getEffectiveState(String taskId, TaskState taskState, TaskState previous) {
        TaskState effectiveState;
        if (previous != null && isTerminal(previous) && taskState == TaskState.TASK_LOST) {
            effectiveState = previous;
            // Replace task status only once (as we cannot remove item from cache, we overwrite the value)
            lastStatusUpdate.put(taskId, taskState);
        } else {
            effectiveState = taskState;
            lastStatusUpdate.put(taskId, taskState);
        }
        return effectiveState;
    }

    private boolean isTerminal(TaskState taskState) {
        return !ACTIVE_MESOS_TASK_STATES.contains(taskState);
    }
}
