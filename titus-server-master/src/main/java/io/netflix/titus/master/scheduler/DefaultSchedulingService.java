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

package io.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.ScaleDownAction;
import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.ScaleDownOrderEvaluator;
import com.netflix.fenzo.ScaleUpAction;
import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.TaskSchedulingService;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.fenzo.queues.TaskQueue;
import com.netflix.fenzo.queues.TaskQueueException;
import com.netflix.fenzo.queues.TaskQueueMultiException;
import com.netflix.fenzo.queues.TaskQueues;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.netflix.spectator.api.patterns.PolledMeter;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import io.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import io.netflix.titus.api.agent.service.AgentManagementFunctions;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobFunctions;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.service.JobManagerException;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.WorkerNaming;
import io.netflix.titus.api.store.v2.InvalidJobException;
import io.netflix.titus.common.framework.fit.FitFramework;
import io.netflix.titus.common.framework.fit.FitInjection;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.common.util.ExceptionExt;
import io.netflix.titus.common.util.guice.annotation.Activator;
import io.netflix.titus.common.util.rx.ObservableExt;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.spectator.SpectatorExt;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.VirtualMachineMasterService;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.job.JobMgr;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.jobmanager.service.JobManagerUtil;
import io.netflix.titus.master.jobmanager.service.TaskInfoFactory;
import io.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import io.netflix.titus.master.model.job.TitusQueuableTask;
import io.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import io.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import io.netflix.titus.master.scheduler.fitness.TitusFitnessCalculator;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheUpdater;
import io.netflix.titus.master.scheduler.scaling.DefaultAutoScaleController;
import io.netflix.titus.master.scheduler.scaling.FenzoAutoScaleRuleWrapper;
import io.netflix.titus.master.store.InvalidJobStateChangeException;
import io.netflix.titus.master.taskmigration.TaskMigrator;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action1;
import rx.schedulers.Schedulers;

import static io.netflix.titus.master.MetricConstants.METRIC_SCHEDULING_SERVICE;

@Singleton
public class DefaultSchedulingService implements SchedulingService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSchedulingService.class);

    private static final String METRIC_SLA_UPDATES = METRIC_SCHEDULING_SERVICE + "slaUpdates";
    private static final long STORE_UPDATE_TIMEOUT_MS = 5_000;
    private static final long vmCurrentStatesCheckIntervalMillis = 10_000L;
    private static final long MAX_DELAY_MILLIS_BETWEEN_SCHEDULING_ITERATIONS = 5_000L;

    private final VirtualMachineMasterService virtualMachineService;
    private final MasterConfiguration config;
    private final SchedulerConfiguration schedulerConfiguration;
    private final V2JobOperations v2JobOperations;
    private final V3JobOperations v3JobOperations;
    private final VMOperations vmOps;
    private final Optional<FitInjection> fitInjection;
    private TaskScheduler taskScheduler;
    private TaskSchedulingService schedulingService;
    private TaskQueue taskQueue;
    private Subscription slaUpdateSubscription;
    // Choose this max delay between scheduling iterations with care. Making it too short makes scheduler do unnecessary
    // work when assignments are not possible. On the other hand, making it too long will delay other aspects such as
    // triggerring autoscale actions, expiring mesos offers, etc.

    private final ConstraintEvaluatorTransformer<JobConstraints> v2ConstraintEvaluatorTransformer;
    private final TaskToClusterMapper taskToClusterMapper = new TaskToClusterMapper();

    private final AtomicLong totalTasksPerIteration;
    private final AtomicLong assignedTasksPerIteration;
    private final AtomicLong failedTasksPerIteration;
    private final AtomicLong offersReceived;
    private final AtomicLong offersRejected;
    private final AtomicLong totalActiveAgents;
    private final AtomicLong totalDisabledAgents;
    private final AtomicLong minDisableDuration;
    private final AtomicLong maxDisableDuration;
    private final AtomicLong totalAvailableCpus;
    private final AtomicLong totalAllocatedCpus;
    private final AtomicLong cpuUtilization;
    private final AtomicLong totalAvailableMemory;
    private final AtomicLong totalAllocatedMemory;
    private final AtomicLong memoryUtilization;
    private final AtomicLong totalAvailableNetworkMbps;
    private final AtomicLong totalAllocatedNetworkMbps;
    private final AtomicLong networkUtilization;
    private final AtomicLong dominantResourceUtilization;

    private final Timer schedulingIterationLatency;

    private final ConcurrentMap<Integer, List<VirtualMachineCurrentState>> vmCurrentStatesMap;
    private final SystemSoftConstraint systemSoftConstraint;
    private final SystemHardConstraint systemHardConstraint;
    private final Scheduler threadScheduler;
    private Action1<QueuableTask> taskQueueAction;
    private final TitusRuntime titusRuntime;
    private final AgentResourceCache agentResourceCache;
    private final AgentResourceCacheUpdater agentResourceCacheUpdater;
    private final BlockingQueue<Map<String, com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>>>>
            taskFailuresActions = new LinkedBlockingQueue<>(5);
    private final TierSlaUpdater tierSlaUpdater;
    private final Registry registry;
    private final TaskMigrator taskMigrator;
    private final AgentManagementService agentManagementService;
    private final DefaultAutoScaleController autoScaleController;
    private final TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory;
    private Subscription vmStateUpdateSubscription;

    @Inject
    public DefaultSchedulingService(V2JobOperations v2JobOperations,
                                    V3JobOperations v3JobOperations,
                                    AgentManagementService agentManagementService,
                                    DefaultAutoScaleController autoScaleController,
                                    TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory,
                                    VMOperations vmOps,
                                    final VirtualMachineMasterService virtualMachineService,
                                    MasterConfiguration config,
                                    SchedulerConfiguration schedulerConfiguration,
                                    SystemSoftConstraint systemSoftConstraint,
                                    SystemHardConstraint systemHardConstraint,
                                    ConstraintEvaluatorTransformer<JobConstraints> v2ConstraintEvaluatorTransformer,
                                    TierSlaUpdater tierSlaUpdater,
                                    Registry registry,
                                    ScaleDownOrderEvaluator scaleDownOrderEvaluator,
                                    Map<ScaleDownConstraintEvaluator, Double> weightedScaleDownConstraintEvaluators,
                                    PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator,
                                    TaskMigrator taskMigrator,
                                    TitusRuntime titusRuntime,
                                    RxEventBus rxEventBus,
                                    AgentResourceCache agentResourceCache) {
        this(v2JobOperations, v3JobOperations, agentManagementService, autoScaleController, v3TaskInfoFactory, vmOps,
                virtualMachineService, config, schedulerConfiguration,
                systemSoftConstraint, systemHardConstraint, v2ConstraintEvaluatorTransformer,
                Schedulers.computation(),
                tierSlaUpdater, registry, scaleDownOrderEvaluator, weightedScaleDownConstraintEvaluators,
                preferentialNamedConsumableResourceEvaluator,
                taskMigrator, titusRuntime, rxEventBus, agentResourceCache
        );
    }

    public DefaultSchedulingService(V2JobOperations v2JobOperations,
                                    V3JobOperations v3JobOperations,
                                    AgentManagementService agentManagementService,
                                    DefaultAutoScaleController autoScaleController,
                                    TaskInfoFactory<Protos.TaskInfo> v3TaskInfoFactory,
                                    VMOperations vmOps,
                                    final VirtualMachineMasterService virtualMachineService,
                                    MasterConfiguration config,
                                    SchedulerConfiguration schedulerConfiguration,
                                    SystemSoftConstraint systemSoftConstraint,
                                    SystemHardConstraint systemHardConstraint,
                                    ConstraintEvaluatorTransformer<JobConstraints> v2ConstraintEvaluatorTransformer,
                                    Scheduler threadScheduler,
                                    TierSlaUpdater tierSlaUpdater,
                                    Registry registry,
                                    ScaleDownOrderEvaluator scaleDownOrderEvaluator,
                                    Map<ScaleDownConstraintEvaluator, Double> weightedScaleDownConstraintEvaluators,
                                    PreferentialNamedConsumableResourceEvaluator preferentialNamedConsumableResourceEvaluator,
                                    TaskMigrator taskMigrator,
                                    TitusRuntime titusRuntime,
                                    RxEventBus rxEventBus,
                                    AgentResourceCache agentResourceCache) {
        this.v2JobOperations = v2JobOperations;
        this.v3JobOperations = v3JobOperations;
        this.agentManagementService = agentManagementService;
        this.autoScaleController = autoScaleController;
        this.v3TaskInfoFactory = v3TaskInfoFactory;
        this.vmOps = vmOps;
        this.virtualMachineService = virtualMachineService;
        this.config = config;
        this.schedulerConfiguration = schedulerConfiguration;
        this.v2ConstraintEvaluatorTransformer = v2ConstraintEvaluatorTransformer;
        this.threadScheduler = threadScheduler;
        this.tierSlaUpdater = tierSlaUpdater;
        this.registry = registry;
        this.taskMigrator = taskMigrator;
        this.titusRuntime = titusRuntime;
        this.agentResourceCache = agentResourceCache;
        this.systemSoftConstraint = systemSoftConstraint;
        this.systemHardConstraint = systemHardConstraint;
        agentResourceCacheUpdater = new AgentResourceCacheUpdater(titusRuntime, agentResourceCache, v3JobOperations, rxEventBus);

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
                .withLeaseOfferExpirySecs(config.getMesosLeaseOfferExpirySecs())
                .withFitnessCalculator(new TitusFitnessCalculator(schedulerConfiguration, agentResourceCache))
                .withFitnessGoodEnoughFunction(TitusFitnessCalculator.fitnessGoodEnoughFunction)
                .withAutoScaleByAttributeName(config.getAutoscaleByAttributeName())
                .withScaleDownOrderEvaluator(scaleDownOrderEvaluator)
                .withWeightedScaleDownConstraintEvaluators(weightedScaleDownConstraintEvaluators)
                .withPreferentialNamedConsumableResourceEvaluator(preferentialNamedConsumableResourceEvaluator)
                .withSchedulingEventListener(new DefaultSchedulingEventListener(titusRuntime, agentResourceCache))
                .withMaxConcurrent(schedulerConfiguration.getSchedulerMaxConcurrent());

        taskScheduler = setupTaskSchedulerAndAutoScaler(virtualMachineService.getLeaseRescindedObservable(), schedulerBuilder);
        taskQueue = TaskQueues.createTieredQueue(2);
        schedulingService = setupTaskSchedulingService(taskScheduler);
        virtualMachineService.setVMLeaseHandler(schedulingService::addLeases);
        taskQueueAction = taskQueue::queueTask;

        totalTasksPerIteration = new AtomicLong(0);
        assignedTasksPerIteration = new AtomicLong(0);
        failedTasksPerIteration = new AtomicLong(0);
        offersReceived = new AtomicLong(0);
        offersRejected = new AtomicLong(0);
        totalActiveAgents = new AtomicLong(0);
        totalDisabledAgents = new AtomicLong(0);
        minDisableDuration = new AtomicLong(0);
        maxDisableDuration = new AtomicLong(0);
        totalAvailableCpus = new AtomicLong(0);
        totalAllocatedCpus = new AtomicLong(0);
        cpuUtilization = new AtomicLong(0);
        totalAvailableMemory = new AtomicLong(0);
        memoryUtilization = new AtomicLong(0);
        totalAllocatedMemory = new AtomicLong(0);
        totalAvailableNetworkMbps = new AtomicLong(0);
        totalAllocatedNetworkMbps = new AtomicLong(0);
        networkUtilization = new AtomicLong(0);
        dominantResourceUtilization = new AtomicLong(0);

        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalTasksPerIteration").monitorValue(totalTasksPerIteration);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "assignedTasksPerIteration").monitorValue(assignedTasksPerIteration);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "failedTasksPerIteration").monitorValue(failedTasksPerIteration);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "offersReceived").monitorValue(offersReceived);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "offersRejected").monitorValue(offersRejected);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalActiveAgents").monitorValue(totalActiveAgents);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalDisabledAgents").monitorValue(totalDisabledAgents);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "minDisableDuration").monitorValue(minDisableDuration);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "maxDisableDuration").monitorValue(maxDisableDuration);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalAvailableCpus").monitorValue(totalAvailableCpus);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalAllocatedCpus").monitorValue(totalAllocatedCpus);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "cpuUtilization").monitorValue(cpuUtilization);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalAvailableMemory").monitorValue(totalAvailableMemory);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalAllocatedMemory").monitorValue(totalAllocatedMemory);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "memoryUtilization").monitorValue(memoryUtilization);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalAvailableNetworkMbps").monitorValue(totalAvailableNetworkMbps);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "totalAllocatedNetworkMbps").monitorValue(totalAllocatedNetworkMbps);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "networkUtilization").monitorValue(networkUtilization);
        PolledMeter.using(registry).withName(METRIC_SCHEDULING_SERVICE + "dominantResourceUtilization").monitorValue(dominantResourceUtilization);

        schedulingIterationLatency = registry.timer(METRIC_SCHEDULING_SERVICE + "schedulingIterationLatency");

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
        if (schedulerConfiguration.isOptimizingShortfallEvaluatorEnabled()) {
            builder.withOptimizingShortfallEvaluator();
        }
        return builder.build();
    }

    @Override
    public SystemSoftConstraint getSystemSoftConstraint() {
        return systemSoftConstraint;
    }

    @Override
    public SystemHardConstraint getSystemHardConstraint() {
        return systemHardConstraint;
    }

    @Override
    public ConstraintEvaluatorTransformer<JobConstraints> getV2ConstraintEvaluatorTransformer() {
        return v2ConstraintEvaluatorTransformer;
    }

    private void setupVmOps(final String attrName) {
        taskScheduler.setActiveVmGroupAttributeName(config.getActiveSlaveAttributeName());
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
                                if (r instanceof ScheduledRequest) {
                                    final WorkerNaming.JobWorkerIdPair j = WorkerNaming.getJobAndWorkerId(r.getId());
                                    s.addJob(new VMOperations.JobOnVMInfo(j.jobId, r.getId()));
                                } else if (r instanceof V3QueueableTask) {
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

    private TaskScheduler setupTaskSchedulerAndAutoScaler(Observable<String> vmLeaseRescindedObservable,
                                                          TaskScheduler.Builder schedulerBuilder) {
        int minMinIdle = 4;
        schedulerBuilder = schedulerBuilder
                .withAutoScalerMapHostnameAttributeName(schedulerConfiguration.getInstanceAttributeName())
                .withDelayAutoscaleUpBySecs(schedulerConfiguration.getDelayAutoScaleUpBySecs())
                .withDelayAutoscaleDownBySecs(schedulerConfiguration.getDelayAutoScaleDownBySecs());
        schedulerBuilder = schedulerBuilder.withMaxOffersToReject(Math.max(1, minMinIdle));
        final TaskScheduler scheduler = schedulerBuilder.build();
        vmLeaseRescindedObservable
                .doOnNext(s -> {
                    if (s.equals("ALL")) {
                        scheduler.expireAllLeases();
                    } else {
                        scheduler.expireLease(s);
                    }
                })
                .subscribe();
        scheduler.setAutoscalerCallback(action -> {
            try {
                switch (action.getType()) {
                    case Up:
                        autoScaleController.handleScaleUpAction(action.getRuleName(), ((ScaleUpAction) action).getScaleUpCount());
                        break;
                    case Down:
                        // The API here is misleading. The 'hosts' attribute of ScaleDownAction contains instance ids.
                        Set<String> idsToTerminate = new HashSet<>(((ScaleDownAction) action).getHosts());
                        Pair<Set<String>, Set<String>> resultPair = autoScaleController.handleScaleDownAction(action.getRuleName(), idsToTerminate);
                        Set<String> notTerminatedInstances = resultPair.getRight();

                        // Now we need to convert instance ids to host names, as this is what the scheduler expects
                        notTerminatedInstances.forEach(id ->
                                ExceptionExt.silent(() -> taskScheduler.enableVM(agentManagementService.getAgentInstance(id).getIpAddress()))
                        );
                        break;
                }
            } catch (Exception e) {
                logger.warn("Will continue after exception calling autoscale action observer: " + e.getMessage(), e);
            }
        });
        return scheduler;
    }

    private void setupAutoscaleRulesDynamicUpdater() {
        titusRuntime.persistentStream(agentManagementService.events(true)).subscribe(
                next -> {
                    try {
                        if (next instanceof AgentInstanceGroupUpdateEvent) {
                            AgentInstanceGroupUpdateEvent updateEvent = (AgentInstanceGroupUpdateEvent) next;
                            AgentInstanceGroup instanceGroup = updateEvent.getAgentInstanceGroup();
                            String instanceGroupId = instanceGroup.getId();
                            io.netflix.titus.api.agent.model.AutoScaleRule rule = instanceGroup.getAutoScaleRule();

                            logger.info("Setting up autoscale rule for the agent instance group {}: {}", instanceGroupId, rule);
                            taskScheduler.addOrReplaceAutoScaleRule(new FenzoAutoScaleRuleWrapper(instanceGroupId, rule));
                        } else if (next instanceof AgentInstanceGroupRemovedEvent) {
                            String instanceGroupId = ((AgentInstanceGroupRemovedEvent) next).getInstanceGroupId();

                            logger.info("Removing autoscale rule for the agent instance group {}", instanceGroupId);
                            taskScheduler.removeAutoScaleRule(instanceGroupId);
                        }
                    } catch (Exception e) {
                        logger.warn("Unexpected error updating cluster autoscale rules: " + e.getMessage());
                    }
                }
        );
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
                        if (logger.isDebugEnabled()) {
                            logger.error(e.getMessage(), e);
                        }
                    }
                },
                vmCurrentStatesCheckIntervalMillis, vmCurrentStatesCheckIntervalMillis,
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void registerTaskQListAction(
            com.netflix.fenzo.functions.Action1<Map<TaskQueue.TaskState, Collection<QueuableTask>>> action
    ) throws IllegalStateException {
        try {
            schedulingService.requestAllTasks(action);
        } catch (TaskQueueException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void registerTaskFailuresAction(
            String taskId, com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>> action
    ) throws IllegalStateException {
        if (!taskFailuresActions.offer(Collections.singletonMap(taskId, action))) {
            throw new IllegalStateException("Too many concurrent requests");
        }
    }

    private void preSchedulingHook() {
        systemHardConstraint.prepare();
        setupTierAutoscalerConfig();
    }

    private void setupTierAutoscalerConfig() {
        taskToClusterMapper.update(agentManagementService);
        schedulingService.setTaskToClusterAutoScalerMapGetter(taskToClusterMapper.getMapperFunc1());
    }

    private void checkIfExitOnSchedError(String s) {
        if (schedulerConfiguration.isExitUponFenzoSchedulingErrorEnabled()) {
            logger.error("Exiting due to fatal error: " + s);
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
                logger.error("Couldn't request state dump from Fenzo: " + e.getMessage(), e);
            }
            try {
                if (!latch.await(MAX_DELAY_MILLIS_BETWEEN_SCHEDULING_ITERATIONS * 3, TimeUnit.MILLISECONDS)) {
                    logger.error("Timeout waiting for Fenzo state dump");
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for Fenzo state dump: " + e.getMessage(), e);
            }
            System.exit(3);
        }
    }

    private void printFenzoStateDump(ObjectMapper mapper, String what, Object dump, CountDownLatch latch) {
        try {
            logger.info("Fenzo state dump of " + what + ": " + mapper.writeValueAsString(dump));
        } catch (JsonProcessingException e) {
            logger.error("Error dumping Fenzo state for " + what + ": " + e.getMessage(), e);
        } finally {
            latch.countDown();
        }
    }

    private void schedulingResultsHandler(SchedulingResult schedulingResult) {
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
        int failedTasksDuringSchedulingResult = 0;

        Map<String, VMAssignmentResult> assignmentResultMap = schedulingResult.getResultMap();
        for (Map.Entry<String, VMAssignmentResult> aResult : assignmentResultMap.entrySet()) {
            Set<TaskAssignmentResult> tasksAssigned = aResult.getValue().getTasksAssigned();
            launchTasks(tasksAssigned, aResult.getValue().getLeasesUsed());
            assignedDuringSchedulingResult += tasksAssigned.size();
        }

        List<Map<String, com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>>>> failActions = new ArrayList<>();
        taskFailuresActions.drainTo(failActions);
        for (Map.Entry<TaskRequest, List<TaskAssignmentResult>> entry : schedulingResult.getFailures().entrySet()) {
            final TitusQueuableTask task = (TitusQueuableTask) entry.getKey();
            failedTasksDuringSchedulingResult++;
            if (!failActions.isEmpty()) {
                final Iterator<Map<String, com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>>>> iterator =
                        failActions.iterator();
                while (iterator.hasNext()) { // iterate over all of them, there could be multiple requests with the same taskId
                    final Map<String, com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>>> next = iterator.next();
                    final String reqId = next.keySet().iterator().next();
                    final com.netflix.fenzo.functions.Action1<List<TaskAssignmentResult>> a = next.values().iterator().next();
                    if (task.getId().equals(reqId)) {
                        a.call(entry.getValue());
                        iterator.remove();
                    }
                }
            }
        }
        if (!failActions.isEmpty()) { // If no such tasks for the registered actions, call them with null result
            failActions.forEach(action -> action.values().iterator().next().call(null));
        }
        totalTasksPerIteration.set(assignedDuringSchedulingResult + failedTasksDuringSchedulingResult);
        assignedTasksPerIteration.set(assignedDuringSchedulingResult);
        failedTasksPerIteration.set(failedTasksDuringSchedulingResult);
        offersReceived.set(schedulingResult.getLeasesAdded());
        offersRejected.set(schedulingResult.getLeasesRejected());
        totalActiveAgents.set(schedulingResult.getTotalVMsCount());
        schedulingIterationLatency.record(schedulingResult.getRuntime(), TimeUnit.MILLISECONDS);
    }

    private void launchTasks(Collection<TaskAssignmentResult> requests, List<VirtualMachineLease> leases) {
        final List<Protos.TaskInfo> taskInfoList = new LinkedList<>();

        long recordStartTime = System.currentTimeMillis();
        try {
            for (TaskAssignmentResult assignmentResult : requests) {
                List<PreferentialNamedConsumableResourceSet.ConsumeResult> consumeResults = assignmentResult.getrSets();
                TitusQueuableTask task = (TitusQueuableTask) assignmentResult.getRequest();

                boolean taskFound;
                PreferentialNamedConsumableResourceSet.ConsumeResult consumeResult = consumeResults.get(0);
                if (JobFunctions.isV2Task(task.getId())) {
                    final JobMgr jobMgr = v2JobOperations.getJobMgrFromTaskId(task.getId());
                    taskFound = jobMgr != null;
                    if (taskFound) {
                        final VirtualMachineLease lease = leases.get(0);
                        try {
                            taskInfoList.add(jobMgr.setLaunchedAndCreateTaskInfo(task, lease.hostname(), getAttributesMap(lease), lease.getOffer().getSlaveId(),
                                    consumeResult, assignmentResult.getAssignedPorts()));
                        } catch (InvalidJobStateChangeException | InvalidJobException e) {
                            logger.warn("Not launching task due to error setting state to launched for " + task.getId() + " - " +
                                    e.getMessage());
                        } catch (Exception e) {
                            // unexpected error creating task info
                            String msg = "fatal error creating taskInfo for " + task.getId() + ": " + e.getMessage();
                            logger.warn("Killing job " + jobMgr.getJobId() + ": " + msg, e);
                            jobMgr.killJob("SYSTEM", msg);
                        }
                    }
                } else { // V3 task
                    Optional<Pair<Job<?>, Task>> v3JobAndTask = v3JobOperations.findTaskById(task.getId());
                    taskFound = v3JobAndTask.isPresent();
                    if (taskFound) {
                        Job v3Job = v3JobAndTask.get().getLeft();
                        Task v3Task = v3JobAndTask.get().getRight();
                        final VirtualMachineLease lease = leases.get(0);
                        try {
                            Map<String, String> attributesMap = getAttributesMap(lease);
                            Protos.TaskInfo taskInfo = v3TaskInfoFactory.newTaskInfo(
                                    task, v3Job, v3Task, lease.hostname(), attributesMap, lease.getOffer().getSlaveId(),
                                    consumeResult);

                            // FIXME This is obvious shortcoming. Failed model updates must be propagated into change action result.
                            fitInjection.ifPresent(i -> i.beforeImmediate("storeLaunchConfiguration"));

                            AtomicReference<Throwable> errorRef = new AtomicReference<>();
                            boolean updated = v3JobOperations.recordTaskPlacement(
                                    task.getId(),
                                    oldTask -> {
                                        try {
                                            return JobManagerUtil.newTaskLaunchConfigurationUpdater(config.getHostZoneAttributeName(), lease, consumeResult, attributesMap).apply(oldTask);
                                        } catch (Exception e) {
                                            errorRef.set(e);
                                            return oldTask;
                                        }
                                    }
                            ).await(STORE_UPDATE_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                            fitInjection.ifPresent(i -> i.afterImmediate("storeLaunchConfiguration"));

                            if (errorRef.get() != null) {
                                if (JobManagerException.hasErrorCode(errorRef.get(), JobManagerException.ErrorCode.UnexpectedTaskState)) {
                                    logger.info("Not launching task, as it is no longer in Accepted state (probably killed)", v3Task.getId());
                                } else {
                                    logger.info("Not launching task {} due to model update failure", v3Task.getId(), errorRef.get());
                                    killBrokenTask(task, "model update error: " + errorRef.get().getMessage());
                                }
                            } else if (updated) {
                                taskInfoList.add(taskInfo);
                            } else {
                                killBrokenTask(task, "store update timeout");
                                logger.error("Timed out during writing task {} (job {}) status update to the store", task.getId(), v3Job.getId());
                            }
                        } catch (Exception e) {
                            killBrokenTask(task, e.toString());
                            logger.error("Fatal error when creating TaskInfo for {}", task.getId(), e);
                        }
                    }
                }
                if (!taskFound) {
                    // job must have been terminated, remove task from Fenzo
                    logger.warn("Rejecting assignment and removing task after not finding jobMgr for " + task.getId());
                    schedulingService.removeTask(task.getId(), task.getQAttributes(), assignmentResult.getHostname());
                }
            }
        } finally {
            logger.info("Recorded task placement decisions in JobManager in {}ms: tasks={}, offers={}", System.currentTimeMillis() - recordStartTime, requests.size(), leases.size());
        }

        long mesosStartTime = System.currentTimeMillis();
        if (taskInfoList.isEmpty()) {
            try {
                leases.forEach(virtualMachineService::rejectLease);
            } finally {
                logger.info("Rejected offers as no task effectively placed on the agent in {}ms: offers={}", System.currentTimeMillis() - mesosStartTime, leases.size());
            }
        } else {
            try {
                virtualMachineService.launchTasks(taskInfoList, leases);
            } finally {
                logger.info("Launched tasks on Mesos in {}ms: tasks={}, offers={}", System.currentTimeMillis() - mesosStartTime, taskInfoList.size(), leases.size());
            }
        }
    }

    private void killBrokenTask(TitusQueuableTask task, String reason) {
        v3JobOperations.killTask(task.getId(), false, String.format("Failed to launch task %s due to %s", task.getId(), reason)).subscribe(
                next -> {
                },
                e -> {
                    if (e instanceof JobManagerException) {
                        JobManagerException je = (JobManagerException) e;

                        // This means task is no longer around, so we can safely ignore this.
                        if (je.getErrorCode() == JobManagerException.ErrorCode.JobNotFound
                                || je.getErrorCode() == JobManagerException.ErrorCode.TaskNotFound
                                || je.getErrorCode() == JobManagerException.ErrorCode.TaskTerminating) {
                            return;
                        }
                    }
                    logger.warn("Attempt to terminate task in potentially inconsistent state due to failed launch process {} failed: {}", task.getId(), e.getMessage());
                },
                () -> logger.warn("Terminated task {} as launch operation could not be completed", task.getId())
        );
    }

    private Map<String, String> getAttributesMap(VirtualMachineLease virtualMachineLease) {
        final Map<String, Protos.Attribute> attributeMap = virtualMachineLease.getAttributeMap();
        final Map<String, String> result = new HashMap<>();
        if (!attributeMap.isEmpty()) {
            for (Map.Entry<String, Protos.Attribute> entry : attributeMap.entrySet()) {
                result.put(entry.getKey(), entry.getValue().getText().getValue());
            }
        }
        return result;
    }

    @Override
    public Action1<QueuableTask> getTaskQueueAction() {
        return taskQueueAction;
    }

    @Override
    public void removeTask(String taskid, QAttributes qAttributes, String hostname) {
        schedulingService.removeTask(taskid, qAttributes, hostname);
    }

    @Override
    public void initRunningTask(QueuableTask task, String hostname) {
        schedulingService.initializeRunningTask(task, hostname);
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        logger.info("Scheduling service starting now");

        setupVmOps(config.getActiveSlaveAttributeName());
        setupAutoscaleRulesDynamicUpdater();

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
    @Override
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

    public List<VirtualMachineCurrentState> getVmCurrentStates() {
        return vmCurrentStatesMap.get(0);
    }

    private void verifyAndReportResourceUsageMetrics(List<VirtualMachineCurrentState> vmCurrentStates) {
        try {
            double totalCpu = 0.0;
            double usedCpu = 0.0;
            double totalMemory = 0.0;
            double usedMemory = 0.0;
            double totalNetworkMbps = 0.0;
            double usedNetworkMbps = 0.0;
            long totalDisabled = 0;
            long currentMinDisableDuration = 0;
            long currentMaxDisableDuration = 0;
            long now = System.currentTimeMillis();

            for (VirtualMachineCurrentState state : vmCurrentStates) {
                final VirtualMachineLease currAvailableResources = state.getCurrAvailableResources();

                if (currAvailableResources != null) {
                    totalCpu += currAvailableResources.cpuCores();
                    totalMemory += currAvailableResources.memoryMB();
                    totalNetworkMbps += currAvailableResources.networkMbps();
                }
                long disableDuration = state.getDisabledUntil() - now;
                if (disableDuration > 0) {
                    totalDisabled++;
                    currentMinDisableDuration = Math.min(currentMinDisableDuration, disableDuration);
                    currentMaxDisableDuration = Math.max(currentMinDisableDuration, disableDuration);
                }
                final Collection<TaskRequest> runningTasks = state.getRunningTasks();
                if (runningTasks != null && !runningTasks.isEmpty()) {
                    for (TaskRequest t : runningTasks) {
                        QueuableTask task = (QueuableTask) t;
                        if (task instanceof ScheduledRequest) {
                            final JobMgr jobMgr = v2JobOperations.getJobMgrFromTaskId(t.getId());
                            if (jobMgr == null || !jobMgr.isTaskValid(t.getId())) {
                                schedulingService.removeTask(task.getId(), task.getQAttributes(), state.getHostname());
                            } else {
                                usedCpu += t.getCPUs();
                                totalCpu += t.getCPUs();
                                usedMemory += t.getMemory();
                                totalMemory += t.getMemory();
                                usedNetworkMbps += t.getNetworkMbps();
                                totalNetworkMbps += t.getNetworkMbps();
                            }
                        } else if (task instanceof V3QueueableTask) {
                            //TODO redo the metrics publishing but we should keep it the same as v2 for now
                            usedCpu += t.getCPUs();
                            totalCpu += t.getCPUs();
                            usedMemory += t.getMemory();
                            totalMemory += t.getMemory();
                            usedNetworkMbps += t.getNetworkMbps();
                            totalNetworkMbps += t.getNetworkMbps();
                        }
                    }
                }
            }

            totalDisabledAgents.set(totalDisabled);
            minDisableDuration.set(currentMinDisableDuration);
            maxDisableDuration.set(currentMaxDisableDuration);
            totalAvailableCpus.set((long) totalCpu);
            totalAllocatedCpus.set((long) usedCpu);
            cpuUtilization.set((long) (usedCpu * 100.0 / Math.max(1.0, totalCpu)));
            double dominantResourceUtilization = usedCpu * 100.0 / totalCpu;
            totalAvailableMemory.set((long) totalMemory);
            totalAllocatedMemory.set((long) usedMemory);
            memoryUtilization.set((long) (usedMemory * 100.0 / Math.max(1.0, totalMemory)));
            dominantResourceUtilization = Math.max(dominantResourceUtilization, usedMemory * 100.0 / totalMemory);
            totalAvailableNetworkMbps.set((long) totalNetworkMbps);
            totalAllocatedNetworkMbps.set((long) usedNetworkMbps);
            networkUtilization.set((long) (usedNetworkMbps * 100.0 / Math.max(1.0, totalNetworkMbps)));
            dominantResourceUtilization = Math.max(dominantResourceUtilization, usedNetworkMbps * 100.0 / totalNetworkMbps);
            this.dominantResourceUtilization.set((long) dominantResourceUtilization);
        } catch (Exception e) {
            logger.error("Error settings metrics with error: ", e);
        }
    }

    private void checkInactiveVMs(List<VirtualMachineCurrentState> vmCurrentStates) {
        logger.debug("Checking on any workers on VMs that are not active anymore");
        List<VirtualMachineCurrentState> inactiveVmStates = VMStateMgr.getInactiveVMs(config.getActiveSlaveAttributeName(), agentManagementService, vmCurrentStates);

        // get all running tasks on the inactive vms
        Collection<TaskRequest> tasksToBeMigrated = inactiveVmStates.stream()
                .flatMap(ivm -> ivm.getRunningTasks().stream())
                .collect(Collectors.toList());

        // schedule the inactive tasks for migration
        taskMigrator.migrate(tasksToBeMigrated);

        // expire all leases on inactive vms
        for (VirtualMachineCurrentState inactiveVmState : inactiveVmStates) {
            VirtualMachineLease lease = inactiveVmState.getCurrAvailableResources();
            String vmHost = lease.hostname();
            logger.info("expiring all leases of inactive vm " + vmHost);
            taskScheduler.expireAllLeases(vmHost);
        }
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(slaUpdateSubscription, vmStateUpdateSubscription);
        taskScheduler.shutdown();
        schedulingService.shutdown();
        agentResourceCacheUpdater.shutdown();
        agentResourceCache.shutdown();
    }

    @Override
    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }
}
