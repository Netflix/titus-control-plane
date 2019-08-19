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

package com.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.archaius.api.Config;
import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet.PreferentialNamedConsumableResource;
import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.TaskSchedulingService;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.queues.TaskQueueException;
import com.netflix.fenzo.queues.TaskQueueMultiException;
import com.netflix.fenzo.queues.TaskQueues;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.titus.api.agent.service.AgentManagementFunctions;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetFunctions;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.framework.fit.FitFramework;
import com.netflix.titus.common.framework.fit.FitInjection;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.SystemExt;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.spectator.SpectatorExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.mesos.LeaseRescindedEvent;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.TaskInfoFactory;
import com.netflix.titus.master.mesos.VirtualMachineMasterService;
import com.netflix.titus.master.model.job.TitusQueuableTask;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.TaskCacheEventListener;
import com.netflix.titus.master.scheduler.fitness.AgentManagementFitnessCalculator;
import com.netflix.titus.master.scheduler.fitness.TitusFitnessCalculator;
import com.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import com.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheUpdater;
import com.netflix.titus.master.scheduler.resourcecache.OpportunisticCpuCache;
import com.netflix.titus.master.scheduler.resourcecache.TaskCache;
import com.netflix.titus.master.taskmigration.TaskMigrator;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;

import static com.netflix.titus.master.MetricConstants.METRIC_SCHEDULING_SERVICE;

@Singleton
public class DefaultSchedulingService implements SchedulingService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchedulingService.class);

    private static final String METRIC_SLA_UPDATES = METRIC_SCHEDULING_SERVICE + "slaUpdates";
    private static final String METRIC_SCHEDULING_ITERATION_LATENCY = METRIC_SCHEDULING_SERVICE + "schedulingIterationLatency";
    private static final long vmCurrentStatesCheckIntervalMillis = 10_000L;

    // Choose this max delay between scheduling iterations with care. Making it too short makes scheduler do unnecessary
    // work when assignments are not possible. On the other hand, making it too long will delay other aspects such as
    // expiring mesos offers, etc.
    private static final long MAX_DELAY_MILLIS_BETWEEN_SCHEDULING_ITERATIONS = 5_000L;

    private final VirtualMachineMasterService virtualMachineService;
    private final MasterConfiguration masterConfiguration;
    private final SchedulerConfiguration schedulerConfiguration;
    private final V3JobOperations v3JobOperations;
    private final VMOperations vmOps;
    private final Optional<FitInjection> fitInjection;
    private final MesosConfiguration mesosConfiguration;
    private TaskScheduler taskScheduler;
    private final TaskSchedulingService schedulingService;
    private TaskQueue taskQueue;
    private Subscription slaUpdateSubscription;
    private final TaskPlacementFailureClassifier taskPlacementFailureClassifier;

    private final Gauge totalTasksPerIterationGauge;
    private final Gauge assignedTasksPerIterationGauge;
    private final Gauge failedTasksPerIterationGauge;
    private final Gauge taskAndAgentEvaluationsPerIterationGauge;
    private final Gauge offersReceivedGauge;
    private final Gauge offersRejectedGauge;
    private final Gauge totalActiveAgentsGauge;
    private final Gauge totalDisabledAgentsGauge;
    private final Gauge minDisableDurationGauge;
    private final Gauge maxDisableDurationGauge;
    private final Gauge totalAvailableCpusGauge;
    private final Gauge totalAllocatedCpusGauge;
    private final Gauge cpuUtilizationGauge;
    private final Gauge totalAvailableMemoryGauge;
    private final Gauge totalAllocatedMemoryGauge;
    private final Gauge memoryUtilizationGauge;
    private final Gauge totalAvailableDiskGauge;
    private final Gauge totalAllocatedDiskGauge;
    private final Gauge diskUtilizationGauge;
    private final Gauge totalAvailableNetworkMbpsGauge;
    private final Gauge totalAllocatedNetworkMbpsGauge;
    private final Gauge networkUtilizationGauge;
    private final Gauge dominantResourceUtilizationGauge;
    private final Gauge totalAvailableNetworkInterfacesGauge;
    private final Gauge totalAllocatedNetworkInterfacesGauge;

    private final Timer fenzoSchedulingResultLatencyTimer;
    private final Timer fenzoCallbackLatencyTimer;
    private final Timer recordTaskPlacementLatencyTimer;
    private final Timer mesosLatencyTimer;

    private final AtomicLong totalSchedulingIterationMesosLatency;

    private final ConcurrentMap<Integer, List<VirtualMachineCurrentState>> vmCurrentStatesMap;
    private final SystemHardConstraint systemHardConstraint;
    private final TaskPlacementRecorder taskPlacementRecorder;
    private final Scheduler threadScheduler;
    private final TitusRuntime titusRuntime;
    private final AgentResourceCache agentResourceCache;
    private final AgentResourceCacheUpdater agentResourceCacheUpdater;
    private final BlockingQueue<Map<String, Action1<List<TaskAssignmentResult>>>> taskFailuresActions = new LinkedBlockingQueue<>(5);
    private final TierSlaUpdater tierSlaUpdater;
    private final Registry registry;
    private final TaskMigrator taskMigrator;
    private final AgentManagementService agentManagementService;
    private Subscription vmStateUpdateSubscription;

    private final AtomicReference<Map<String, List<TaskAssignmentResult>>> lastSchedulingResult = new AtomicReference<>();
    private final BehaviorSubject<Map<String, List<TaskAssignmentResult>>> schedulingResultSubject = BehaviorSubject.create();

    @Inject
    public DefaultSchedulingService(V3JobOperations v3JobOperations,
                                    AgentManagementService agentManagementService,
                                    TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory,
                                    VMOperations vmOps,
                                    final VirtualMachineMasterService virtualMachineService,
                                    MasterConfiguration masterConfiguration,
                                    SchedulerConfiguration schedulerConfiguration,
                                    SystemHardConstraint systemHardConstraint,
                                    TaskCache taskCache,
                                    OpportunisticCpuCache opportunisticCpuCache,
                                    TierSlaUpdater tierSlaUpdater,
                                    Registry registry,
                                    PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator,
                                    AgentManagementFitnessCalculator agentManagementFitnessCalculator,
                                    TaskMigrator taskMigrator,
                                    TitusRuntime titusRuntime,
                                    AgentResourceCache agentResourceCache,
                                    Config config,
                                    MesosConfiguration mesosConfiguration) {
        this(v3JobOperations, agentManagementService, v3TaskInfoFactory, vmOps, virtualMachineService,
                masterConfiguration, schedulerConfiguration, systemHardConstraint, taskCache, opportunisticCpuCache,
                Schedulers.computation(), tierSlaUpdater, registry, preferentialNamedConsumableResourceEvaluator,
                agentManagementFitnessCalculator, taskMigrator, titusRuntime, agentResourceCache, config,
                mesosConfiguration);
    }

    public DefaultSchedulingService(V3JobOperations v3JobOperations,
                                    AgentManagementService agentManagementService,
                                    TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory,
                                    VMOperations vmOps,
                                    final VirtualMachineMasterService virtualMachineService,
                                    MasterConfiguration masterConfiguration,
                                    SchedulerConfiguration schedulerConfiguration,
                                    SystemHardConstraint systemHardConstraint,
                                    TaskCache taskCache,
                                    OpportunisticCpuCache opportunisticCpuCache,
                                    Scheduler threadScheduler,
                                    TierSlaUpdater tierSlaUpdater,
                                    Registry registry,
                                    PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator,
                                    AgentManagementFitnessCalculator agentManagementFitnessCalculator,
                                    TaskMigrator taskMigrator,
                                    TitusRuntime titusRuntime,
                                    AgentResourceCache agentResourceCache,
                                    Config config,
                                    MesosConfiguration mesosConfiguration) {
        this.v3JobOperations = v3JobOperations;
        this.agentManagementService = agentManagementService;
        this.vmOps = vmOps;
        this.virtualMachineService = virtualMachineService;
        this.masterConfiguration = masterConfiguration;
        this.schedulerConfiguration = schedulerConfiguration;
        this.threadScheduler = threadScheduler;
        this.tierSlaUpdater = tierSlaUpdater;
        this.registry = registry;
        this.taskMigrator = taskMigrator;
        this.titusRuntime = titusRuntime;
        this.agentResourceCache = agentResourceCache;
        this.systemHardConstraint = systemHardConstraint;
        this.mesosConfiguration = mesosConfiguration;
        agentResourceCacheUpdater = new AgentResourceCacheUpdater(titusRuntime, agentResourceCache, v3JobOperations);

        FitFramework fit = titusRuntime.getFitFramework();
        if (fit.isActive()) {
            this.fitInjection = Optional.of(fit.newFitInjectionBuilder("taskLaunchAndStore")
                    .withDescription("Break write to store during Fenzo task launch")
                    .build()
            );
            fit.getRootComponent().getChild(COMPONENT).addInjection(fitInjection.get());
        } else {
            this.fitInjection = Optional.empty();
        }

        TaskScheduler.Builder schedulerBuilder = new TaskScheduler.Builder()
                .withLeaseRejectAction(virtualMachineService::rejectLease)
                .withLeaseOfferExpirySecs(masterConfiguration.getMesosLeaseOfferExpirySecs())
                .withFitnessCalculator(new TitusFitnessCalculator(schedulerConfiguration, agentManagementFitnessCalculator, agentResourceCache))
                .withFitnessGoodEnoughFunction(TitusFitnessCalculator.fitnessGoodEnoughFunction)
                .withPreferentialNamedConsumableResourceEvaluator(preferentialNamedConsumableResourceEvaluator)
                .withMaxConcurrent(schedulerConfiguration.getSchedulerMaxConcurrent())
                .withTaskBatchSizeSupplier(schedulerConfiguration::getTaskBatchSize)
                .withSchedulingEventListener(new TaskCacheEventListener(taskCache, opportunisticCpuCache, titusRuntime));

        taskScheduler = setupTaskScheduler(virtualMachineService.getLeaseRescindedObservable(), schedulerBuilder);
        taskQueue = TaskQueues.createTieredQueue(2);
        schedulingService = setupTaskSchedulingService(taskScheduler);
        virtualMachineService.setVMLeaseHandler(schedulingService::addLeases);
        virtualMachineService.setRescindLeaseHandler(leaseRescindedEvents -> {
            for (LeaseRescindedEvent event : leaseRescindedEvents) {
                switch (event.getType()) {
                    case All:
                        taskScheduler.expireAllLeases();
                        break;
                    case LeaseId:
                        taskScheduler.expireLease(event.getValue());
                        break;
                }
            }
        });

        this.taskPlacementRecorder = new TaskPlacementRecorder(config, masterConfiguration, schedulingService, v3JobOperations, v3TaskInfoFactory, opportunisticCpuCache, titusRuntime);
        this.taskPlacementFailureClassifier = new TaskPlacementFailureClassifier(titusRuntime);

        totalTasksPerIterationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalTasksPerIteration");
        assignedTasksPerIterationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "assignedTasksPerIteration");
        failedTasksPerIterationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "failedTasksPerIteration");
        taskAndAgentEvaluationsPerIterationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "taskAndAgentEvaluationsPerIteration");
        offersReceivedGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "offersReceived");
        offersRejectedGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "offersRejected");
        totalActiveAgentsGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalActiveAgents");
        totalDisabledAgentsGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalDisabledAgents");
        minDisableDurationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "minDisableDuration");
        maxDisableDurationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "maxDisableDuration");
        totalAvailableCpusGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAvailableCpus");
        totalAllocatedCpusGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAllocatedCpus");
        cpuUtilizationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "cpuUtilization");
        totalAvailableMemoryGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAvailableMemory");
        totalAllocatedMemoryGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAllocatedMemory");
        memoryUtilizationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "memoryUtilization");
        totalAvailableDiskGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAvailableDisk");
        totalAllocatedDiskGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAllocatedDisk");
        diskUtilizationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "diskUtilization");
        totalAvailableNetworkMbpsGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAvailableNetworkMbps");
        totalAllocatedNetworkMbpsGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAllocatedNetworkMbps");
        networkUtilizationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "networkUtilization");
        dominantResourceUtilizationGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "dominantResourceUtilization");
        totalAvailableNetworkInterfacesGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAvailableNetworkInterfaces");
        totalAllocatedNetworkInterfacesGauge = registry.gauge(METRIC_SCHEDULING_SERVICE + "totalAllocatedNetworkInterfaces");

        fenzoSchedulingResultLatencyTimer = registry.timer(METRIC_SCHEDULING_ITERATION_LATENCY, "section", "fenzoSchedulingResult");
        fenzoCallbackLatencyTimer = registry.timer(METRIC_SCHEDULING_ITERATION_LATENCY, "section", "fenzoCallback");
        recordTaskPlacementLatencyTimer = registry.timer(METRIC_SCHEDULING_ITERATION_LATENCY, "section", "recordTaskPlacement");
        mesosLatencyTimer = registry.timer(METRIC_SCHEDULING_ITERATION_LATENCY, "section", "mesos");

        totalSchedulingIterationMesosLatency = new AtomicLong();

        vmCurrentStatesMap = new ConcurrentHashMap<>();
    }

    private TaskSchedulingService setupTaskSchedulingService(TaskScheduler taskScheduler) {
        TaskSchedulingService.Builder builder = new TaskSchedulingService.Builder()
                .withLoopIntervalMillis(schedulerConfiguration.getSchedulerIterationIntervalMs())
                .withMaxDelayMillis(MAX_DELAY_MILLIS_BETWEEN_SCHEDULING_ITERATIONS) // sort of rate limiting when no assignments were made and no new offers available
                .withTaskQueue(taskQueue)
                .withPreSchedulingLoopHook(this::preSchedulingHook)
                .withSchedulingResultCallback(this::schedulingResultsHandler)
                .withTaskScheduler(taskScheduler);
        return builder.build();
    }

    private void setupVmOps(final String attrName) {
        taskScheduler.setActiveVmGroupAttributeName(masterConfiguration.getActiveSlaveAttributeName());
        vmOps.setJobsOnVMsGetter(() -> {
            List<VMOperations.JobsOnVMStatus> result = new ArrayList<>();
            final List<VirtualMachineCurrentState> vmCurrentStates = vmCurrentStatesMap.get(0);
            if (vmCurrentStates != null && !vmCurrentStates.isEmpty()) {
                for (VirtualMachineCurrentState currentState : vmCurrentStates) {
                    final VirtualMachineLease currAvailableResources = currentState.getCurrAvailableResources();
                    if (currAvailableResources != null) {
                        final Protos.Attribute attribute = currAvailableResources.getAttributeMap().get(attrName);
                        if (attribute != null) {
                            VMOperations.JobsOnVMStatus s =
                                    new VMOperations.JobsOnVMStatus(currAvailableResources.hostname(),
                                            attribute.getText().getValue());
                            for (TaskRequest r : currentState.getRunningTasks()) {
                                if (r instanceof V3QueueableTask) {
                                    V3QueueableTask v3Task = (V3QueueableTask) r;
                                    s.addJob(new VMOperations.JobOnVMInfo(v3Task.getJob().getId(), v3Task.getId()));
                                }
                            }
                            result.add(s);
                        }
                    }
                }
            }
            return result;
        });

        titusRuntime.persistentStream(AgentManagementFunctions.observeActiveInstanceGroupIds(agentManagementService))
                .subscribe(ids -> {
                    taskScheduler.setActiveVmGroups(ids);
                    logger.info("Updating Fenzo taskScheduler active instance group list to: {}", ids);
                });
    }

    private TaskScheduler setupTaskScheduler(Observable<LeaseRescindedEvent> vmLeaseRescindedObservable,
                                             TaskScheduler.Builder schedulerBuilder) {
        int minMinIdle = 4;
        final TaskScheduler scheduler = schedulerBuilder.withMaxOffersToReject(Math.max(1, minMinIdle))
                .withSingleOfferPerVM(mesosConfiguration.isKubeApiServerIntegrationEnabled())
                .build();
        vmLeaseRescindedObservable
                .doOnNext(event -> {
                    switch (event.getType()) {
                        case All:
                            scheduler.expireAllLeases();
                            break;
                        case LeaseId:
                            scheduler.expireLease(event.getValue());
                            break;
                    }
                })
                .subscribe();
        return scheduler;
    }

    private void setupVmStatesUpdate() {
        this.vmStateUpdateSubscription = threadScheduler.createWorker().schedulePeriodically(
                () -> {
                    try {
                        schedulingService.requestVmCurrentStates(
                                states -> {
                                    vmCurrentStatesMap.put(0, states);
                                    verifyAndReportResourceUsageMetrics(states);
                                    checkInactiveVMs(states);
                                    vmOps.setAgentInfos(states);
                                }
                        );
                    } catch (TaskQueueException e) {
                        logger.error(e.getMessage());
                        logger.debug(e.getMessage(), e);
                    }
                },
                vmCurrentStatesCheckIntervalMillis, vmCurrentStatesCheckIntervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    private void preSchedulingHook() {
        systemHardConstraint.prepare();
    }

    private void checkIfExitOnSchedError(String s) {
        if (schedulerConfiguration.isExitUponFenzoSchedulingErrorEnabled()) {
            logger.error("Exiting due to fatal error: {}", s);
            CountDownLatch latch = new CountDownLatch(3);
            final ObjectMapper mapper = new ObjectMapper();
            try {
                schedulingService.requestVmCurrentStates(currentStates ->
                        printFenzoStateDump(mapper, "agent current states", currentStates, latch));
                schedulingService.requestAllTasks(taskStateCollectionMap ->
                        printFenzoStateDump(mapper, "task queue", taskStateCollectionMap, latch));
                schedulingService.requestResourceStatus(resourceStatus ->
                        printFenzoStateDump(mapper, "resource status", resourceStatus, latch));
            } catch (TaskQueueException e) {
                logger.error("Couldn't request state dump from Fenzo: {}", e.getMessage(), e);
            }
            try {
                if (!latch.await(MAX_DELAY_MILLIS_BETWEEN_SCHEDULING_ITERATIONS * 3, TimeUnit.MILLISECONDS)) {
                    logger.error("Timeout waiting for Fenzo state dump");
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for Fenzo state dump: {}", e.getMessage(), e);
            }
            SystemExt.forcedProcessExit(3);
        }
    }

    private void printFenzoStateDump(ObjectMapper mapper, String what, Object dump, CountDownLatch latch) {
        try {
            // Although writeValueAsString() is potentially expensive, it only
            // happens once, right before the JVM exits. Therefore, an isInfoEnabled()
            // guard is not needed.
            logger.info("Fenzo state dump of {}: {}", what, mapper.writeValueAsString(dump));
        } catch (JsonProcessingException e) {
            logger.error("Error dumping Fenzo state for {}: {}", what, e.getMessage(), e);
        } finally {
            latch.countDown();
        }
    }

    private void schedulingResultsHandler(SchedulingResult schedulingResult) {
        logger.debug("Task placement results: taskAndAgentEvaluations={}, executionTimeMs={}",
                schedulingResult.getNumAllocations(), schedulingResult.getRuntime());
        long callbackStart = titusRuntime.getClock().wallTime();
        totalSchedulingIterationMesosLatency.set(0);

        if (!schedulingResult.getExceptions().isEmpty()) {
            logger.error("Exceptions in scheduling iteration:");
            for (Exception e : schedulingResult.getExceptions()) {
                if (e instanceof TaskQueueMultiException) {
                    for (Exception ee : ((TaskQueueMultiException) e).getExceptions()) {
                        logger.error(ee.getMessage(), ee);
                    }
                } else {
                    logger.error(e.getMessage(), e);
                }
            }
            checkIfExitOnSchedError("One or more errors in Fenzo scheduling iteration");
            return;
        }

        int assignedDuringSchedulingResult = 0;
        int failedTasksDuringSchedulingResult = schedulingResult.getFailures().size();

        long recordingStart = titusRuntime.getClock().wallTime();
        List<Pair<List<VirtualMachineLease>, List<Protos.TaskInfo>>> taskInfos = taskPlacementRecorder.record(schedulingResult);
        recordTaskPlacementLatencyTimer.record(titusRuntime.getClock().wallTime() - recordingStart, TimeUnit.MILLISECONDS);
        taskInfos.forEach(ts -> launchTasks(ts.getLeft(), ts.getRight()));
        assignedDuringSchedulingResult += taskInfos.stream().mapToInt(p -> p.getRight().size()).sum();

        recordLastSchedulingResult(schedulingResult);
        processTaskSchedulingFailureCallbacks(schedulingResult);
        taskPlacementFailureClassifier.update(schedulingResult);

        totalTasksPerIterationGauge.set(assignedDuringSchedulingResult + failedTasksDuringSchedulingResult);
        assignedTasksPerIterationGauge.set(assignedDuringSchedulingResult);
        failedTasksPerIterationGauge.set(failedTasksDuringSchedulingResult);
        taskAndAgentEvaluationsPerIterationGauge.set(schedulingResult.getNumAllocations());
        offersReceivedGauge.set(schedulingResult.getLeasesAdded());
        offersRejectedGauge.set(schedulingResult.getLeasesRejected());
        totalActiveAgentsGauge.set(schedulingResult.getTotalVMsCount());
        fenzoSchedulingResultLatencyTimer.record(schedulingResult.getRuntime(), TimeUnit.MILLISECONDS);
        fenzoCallbackLatencyTimer.record(titusRuntime.getClock().wallTime() - callbackStart, TimeUnit.MILLISECONDS);
        mesosLatencyTimer.record(totalSchedulingIterationMesosLatency.get(), TimeUnit.MILLISECONDS);
    }

    private void recordLastSchedulingResult(SchedulingResult schedulingResult) {
        try {
            Map<String, List<TaskAssignmentResult>> byTaskId = new HashMap<>();
            schedulingResult.getResultMap().forEach((agentId, vmAssignments) ->
                    vmAssignments.getTasksAssigned().forEach(ta -> byTaskId.put(ta.getTaskId(), Collections.singletonList(ta)))
            );
            schedulingResult.getFailures().forEach((taskRequest, taskAssignments) ->
                    byTaskId.put(taskRequest.getId(), taskAssignments)
            );
            this.lastSchedulingResult.set(byTaskId);
            this.schedulingResultSubject.onNext(byTaskId);
        } catch (Exception e) {
            logger.warn("Failed to record the last scheduling decision", e);
        }
    }

    //TODO(fabio): dead code, delete this
    private void processTaskSchedulingFailureCallbacks(SchedulingResult schedulingResult) {
        List<Map<String, Action1<List<TaskAssignmentResult>>>> failActions = new ArrayList<>();
        taskFailuresActions.drainTo(failActions);

        if (failActions.isEmpty()) {
            return;
        }

        for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry : schedulingResult.getFailures().entrySet()) {
            final TitusQueuableTask task = (TitusQueuableTask) entry.getKey();
            final Iterator<Map<String, Action1<List<TaskAssignmentResult>>>> iterator = failActions.iterator();
            while (iterator.hasNext()) { // iterate over all of them, there could be multiple requests with the same taskId
                final Map<String, Action1<List<TaskAssignmentResult>>> next = iterator.next();
                final String reqId = next.keySet().iterator().next();
                final Action1<List<TaskAssignmentResult>> a = next.values().iterator().next();
                if (task.getId().equals(reqId)) {
                    a.call(entry.getValue());
                    iterator.remove();
                }
            }
        }
        if (!failActions.isEmpty()) { // If no such tasks for the registered actions, call them with null result
            failActions.forEach(action -> action.values().iterator().next().call(null));
        }
    }

    private void launchTasks(List<VirtualMachineLease> leases, List<Protos.TaskInfo> taskInfoList) {
        long mesosStartTime = titusRuntime.getClock().wallTime();
        if (taskInfoList.isEmpty()) {
            try {
                leases.forEach(virtualMachineService::rejectLease);
            } finally {
                long mesosLatency = titusRuntime.getClock().wallTime() - mesosStartTime;
                totalSchedulingIterationMesosLatency.addAndGet(mesosLatency);
                logger.info("Rejected offers as no task effectively placed on the agent in {}ms: offers={}", mesosLatency, leases.size());
            }
        } else {
            try {
                virtualMachineService.launchTasks(taskInfoList, leases);
            } finally {
                long mesosLatency = titusRuntime.getClock().wallTime() - mesosStartTime;
                totalSchedulingIterationMesosLatency.addAndGet(mesosLatency);
                logger.info("Launched tasks on Mesos in {}ms: tasks={}, offers={}", mesosLatency, taskInfoList.size(), leases.size());
            }
        }
    }

    @Override
    public void addTask(QueuableTask queuableTask) {
        logger.info("Adding task to Fenzo: taskId={}, qAttributes={}", queuableTask.getId(), queuableTask.getQAttributes());
        taskQueue.queueTask(queuableTask);
    }

    @Override
    public void removeTask(String taskId, QAttributes qAttributes, String hostname) {
        logger.info("Removing task from Fenzo: taskId={}, qAttributes={}, hostname={}", taskId, qAttributes, hostname);
        schedulingService.removeTask(taskId, qAttributes, hostname);
    }

    @Override
    public void addRunningTask(QueuableTask task, String hostname) {
        logger.info("Initializing Fenzo with the task: taskId={}, qAttributes={}, host={}", task.getId(), task.getQAttributes(), hostname);
        schedulingService.initializeRunningTask(task, hostname);
        agentResourceCacheUpdater.createOrUpdateAgentResourceCacheForTask(task, hostname);
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        logger.info("Scheduling service starting now");

        setupVmOps(masterConfiguration.getActiveSlaveAttributeName());

        this.slaUpdateSubscription = tierSlaUpdater.tieredQueueSlaUpdates()
                .compose(SpectatorExt.subscriptionMetrics(METRIC_SLA_UPDATES, DefaultSchedulingService.class, registry))
                .subscribe(
                        update -> {
                            try {
                                taskQueue.setSla(update);
                            } catch (Throwable e) {
                                logger.error("Unexpected error in SLA update routine", e);
                            }
                        },
                        e -> logger.error("Unexpected error in SLA update routine", e)
                );

        return Observable.empty();
    }

    /**
     * FIXME Due to circular dependencies, we cannot depend on the activation framework to do the initialization in the right order.
     * To fix this problem we have to redesign our APIs.
     */
    public void startScheduling() {
        if (schedulerConfiguration.isSchedulerEnabled()) {
            logger.info("Starting the scheduling service");
            schedulingService.start();
        } else {
            logger.info("Not starting the scheduling service");
        }

        setupVmStatesUpdate();
        agentResourceCacheUpdater.start();
    }

    public Optional<SchedulingResultEvent> findLastSchedulingResult(String taskId) {
        if (lastSchedulingResult.get() == null) {
            return Optional.empty();
        }

        Optional<Pair<Job<?>, Task>> jobTaskOptional = v3JobOperations.findTaskById(taskId);
        if (!jobTaskOptional.isPresent()) {
            return Optional.empty();
        }
        Task task = jobTaskOptional.get().getRight();

        if (task.getStatus().getState() != TaskState.Accepted) {
            return Optional.of(SchedulingResultEvent.onStarted(task));
        }

        List<TaskAssignmentResult> taskAssignmentResults = lastSchedulingResult.get().get(taskId);
        if (taskAssignmentResults == null) {
            return Optional.of(SchedulingResultEvent.onNoAgent(task));
        }
        if (taskAssignmentResults.isEmpty()) {
            throw new IllegalStateException("Unexpected to find empty task assignment list: " + taskId);
        }
        if (taskAssignmentResults.size() == 1 && taskAssignmentResults.get(0).isSuccessful()) {
            // If just launched, the state in job manager is not updated yet. We fix it here to reflect this change in the event.
            Task currentTask = task.toBuilder().withStatus(
                    TaskStatus.newBuilder()
                            .withState(TaskState.Launched)
                            .withReasonCode(TaskStatus.REASON_NORMAL)
                            .withReasonMessage("Launched by scheduler")
                            .build()
            ).build();
            return Optional.of(SchedulingResultEvent.onStarted(currentTask));
        }

        // Failures
        return Optional.of(SchedulingResultEvent.onFailure(task, taskAssignmentResults));
    }

    @Override
    public Observable<SchedulingResultEvent> observeSchedulingResults(String taskId) {
        return schedulingResultSubject
                .flatMap(schedulingResult -> {
                    try {
                        return findLastSchedulingResult(taskId)
                                .map(Observable::just)
                                .orElseGet(() -> Observable.error(new IllegalArgumentException("Task not found: " + taskId)));
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                })
                .takeUntil(event -> event.getTask().getStatus().getState() != TaskState.Accepted);
    }

    @Override
    public Map<TaskPlacementFailure.FailureKind, Map<String, List<TaskPlacementFailure>>> getLastTaskPlacementFailures() {
        return taskPlacementFailureClassifier.getLastTaskPlacementFailures();
    }

    private void verifyAndReportResourceUsageMetrics(List<VirtualMachineCurrentState> vmCurrentStates) {
        try {
            double totalCpu = 0.0;
            double usedCpu = 0.0;
            double totalMemory = 0.0;
            double usedMemory = 0.0;
            double totalDisk = 0.0;
            double usedDisk = 0.0;
            double totalNetworkMbps = 0.0;
            double usedNetworkMbps = 0.0;
            long totalNetworkInterfaces = 0;
            long usedNetworkInterfaces = 0;
            long totalDisabled = 0;
            long currentMinDisableDuration = 0;
            long currentMaxDisableDuration = 0;
            long now = titusRuntime.getClock().wallTime();

            for (VirtualMachineCurrentState state : vmCurrentStates) {
                for (PreferentialNamedConsumableResourceSet set : state.getResourceSets().values()) {
                    if (set.getName().equalsIgnoreCase("enis")) {
                        List<PreferentialNamedConsumableResource> usageBy = set.getUsageBy();
                        totalNetworkInterfaces += usageBy.size();
                        for (PreferentialNamedConsumableResource consumableResource : usageBy) {
                            if (!consumableResource.getUsageBy().isEmpty()) {
                                usedNetworkInterfaces++;
                            }
                        }
                    }
                }

                final VirtualMachineLease currAvailableResources = state.getCurrAvailableResources();
                if (currAvailableResources != null) {
                    totalCpu += currAvailableResources.cpuCores();
                    totalMemory += currAvailableResources.memoryMB();
                    totalDisk += currAvailableResources.diskMB();
                    totalNetworkMbps += currAvailableResources.networkMbps();
                }

                long disableDuration = state.getDisabledUntil() - now;
                if (disableDuration > 0) {
                    totalDisabled++;
                    currentMinDisableDuration = Math.min(currentMinDisableDuration, disableDuration);
                    currentMaxDisableDuration = Math.max(currentMaxDisableDuration, disableDuration);
                }
                final Collection<TaskRequest> runningTasks = state.getRunningTasks();
                if (runningTasks != null && !runningTasks.isEmpty()) {
                    for (TaskRequest t : runningTasks) {
                        QueuableTask task = (QueuableTask) t;
                        if (task instanceof V3QueueableTask) {
                            //TODO redo the metrics publishing but we should keep it the same as v2 for now
                            usedCpu += t.getCPUs();
                            totalCpu += t.getCPUs();
                            usedMemory += t.getMemory();
                            totalMemory += t.getMemory();
                            usedDisk += t.getDisk();
                            totalDisk += t.getDisk();
                            usedNetworkMbps += t.getNetworkMbps();
                            totalNetworkMbps += t.getNetworkMbps();
                        }
                    }
                }
            }

            totalDisabledAgentsGauge.set(totalDisabled);
            minDisableDurationGauge.set(currentMinDisableDuration);
            maxDisableDurationGauge.set(currentMaxDisableDuration);
            totalAvailableCpusGauge.set((long) totalCpu);
            totalAllocatedCpusGauge.set((long) usedCpu);
            cpuUtilizationGauge.set((long) (usedCpu * 100.0 / Math.max(1.0, totalCpu)));
            double dominantResourceUtilization = usedCpu * 100.0 / totalCpu;
            totalAvailableMemoryGauge.set((long) totalMemory);
            totalAllocatedMemoryGauge.set((long) usedMemory);
            memoryUtilizationGauge.set((long) (usedMemory * 100.0 / Math.max(1.0, totalMemory)));
            dominantResourceUtilization = Math.max(dominantResourceUtilization, usedMemory * 100.0 / totalMemory);
            totalAvailableDiskGauge.set((long) totalDisk);
            totalAllocatedDiskGauge.set((long) usedDisk);
            diskUtilizationGauge.set((long) (usedDisk * 100.0 / Math.max(1.0, totalDisk)));
            dominantResourceUtilization = Math.max(dominantResourceUtilization, usedDisk * 100.0 / totalDisk);
            totalAvailableNetworkMbpsGauge.set((long) totalNetworkMbps);
            totalAllocatedNetworkMbpsGauge.set((long) usedNetworkMbps);
            networkUtilizationGauge.set((long) (usedNetworkMbps * 100.0 / Math.max(1.0, totalNetworkMbps)));
            dominantResourceUtilization = Math.max(dominantResourceUtilization, usedNetworkMbps * 100.0 / totalNetworkMbps);
            this.dominantResourceUtilizationGauge.set((long) dominantResourceUtilization);
            totalAvailableNetworkInterfacesGauge.set(totalNetworkInterfaces);
            totalAllocatedNetworkInterfacesGauge.set(usedNetworkInterfaces);
        } catch (Exception e) {
            logger.error("Error settings metrics with error: ", e);
        }
    }

    private void checkInactiveVMs(List<VirtualMachineCurrentState> vmCurrentStates) {
        logger.debug("Checking on any workers on VMs that are not active anymore");
        List<VirtualMachineCurrentState> inactiveVmStates = VMStateMgr.getInactiveVMs(schedulerConfiguration.getInstanceAttributeName(),
                agentManagementService, vmCurrentStates);

        // get all running tasks on the inactive vms
        Collection<TaskRequest> tasksToBeMigrated = inactiveVmStates.stream()
                .flatMap(ivm -> ivm.getRunningTasks().stream())
                .filter(this::isLegacyJob)
                .collect(Collectors.toList());

        // schedule the inactive tasks for migration
        taskMigrator.migrate(tasksToBeMigrated);

        // expire all leases on inactive vms
        for (VirtualMachineCurrentState inactiveVmState : inactiveVmStates) {
            VirtualMachineLease lease = inactiveVmState.getCurrAvailableResources();
            String vmHost = lease.hostname();
            logger.debug("expiring all leases of inactive vm {}", vmHost);
            taskScheduler.expireAllLeases(vmHost);
        }
    }

    private boolean isLegacyJob(TaskRequest taskRequest) {
        return v3JobOperations.findTaskById(taskRequest.getId())
                .filter(p -> DisruptionBudgetFunctions.isLegacyJob(p.getLeft()))
                .isPresent();
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(slaUpdateSubscription, vmStateUpdateSubscription);
        taskScheduler.shutdown();
        schedulingService.shutdown();
        agentResourceCacheUpdater.shutdown();
        agentResourceCache.shutdown();
    }

}
