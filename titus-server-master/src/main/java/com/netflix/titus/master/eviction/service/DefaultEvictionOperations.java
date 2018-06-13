package com.netflix.titus.master.eviction.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.eviction.model.EvictionQuota;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.model.event.EvictionEvent;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.eviction.service.EvictionOperations;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobFunctions;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.FixedIntervalTokenBucketRefillPolicy;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.model.TokenBucketPolicies;
import com.netflix.titus.api.model.TokenBucketPolicy;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.ManagementSubsystemInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.Subscription;
import rx.subjects.PublishSubject;
import rx.subjects.SerializedSubject;
import rx.subjects.Subject;

@Singleton
public class DefaultEvictionOperations implements EvictionOperations {

    private static final Logger logger = LoggerFactory.getLogger(DefaultEvictionOperations.class);

    private static final SystemDisruptionBudget GLOBAL_DISRUPTION_BUDGET = SystemDisruptionBudget.newBuilder()
            .withReference(Reference.global())
            .withTokenBucketDescriptor(
                    TokenBucketPolicy.newBuilder()
                            .withCapacity(20)
                            .withInitialNumberOfTokens(20)
                            .withRefillPolicy(FixedIntervalTokenBucketRefillPolicy.newBuilder()
                                    .withIntervalMs(10_000)
                                    .withNumberOfTokensPerInterval(5)
                                    .build()
                            )
                            .build()
            )
            .build();

    private static final SystemDisruptionBudget TIER_DISRUPTION_BUDGET = SystemDisruptionBudget.newBuilder()
            .withReference(Reference.global())
            .withTokenBucketDescriptor(
                    TokenBucketPolicy.newBuilder()
                            .withCapacity(10)
                            .withInitialNumberOfTokens(10)
                            .withRefillPolicy(FixedIntervalTokenBucketRefillPolicy.newBuilder()
                                    .withIntervalMs(10_000)
                                    .withNumberOfTokensPerInterval(2)
                                    .build()
                            )
                            .build()
            )
            .build();

    private static final SystemDisruptionBudget DEFAULT_CAPACITY_GROUP_DISRUPTION_BUDGET = SystemDisruptionBudget.newBuilder()
            .withReference(Reference.global())
            .withTokenBucketDescriptor(
                    TokenBucketPolicy.newBuilder()
                            .withCapacity(5)
                            .withInitialNumberOfTokens(5)
                            .withRefillPolicy(FixedIntervalTokenBucketRefillPolicy.newBuilder()
                                    .withIntervalMs(10_000)
                                    .withNumberOfTokensPerInterval(1)
                                    .build()
                            )
                            .build()
            )
            .build();

    private static final long QUOTA_UPDATE_INTERVAL_MS = 100;

    private final V3JobOperations jobOperations;
    private final ApplicationSlaManagementService capacityGroupService;

    private final TokenBucket globalTokenBucket = newTokenBucket("global", GLOBAL_DISRUPTION_BUDGET);

    private final ConcurrentMap<Tier, TokenBucket> tierTokenBuckets = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, TokenBucket> capacityGroupTokenBuckets = new ConcurrentHashMap<>();

    private final Object quotaLock = new Object();

    private final Subject<EvictionEvent, EvictionEvent> eventSubject = new SerializedSubject<>(PublishSubject.create());
    private final Observable<EvictionEvent> eventObservable = ObservableExt.protectFromMissingExceptionHandlers(eventSubject, logger);

    private final ConcurrentMap<Reference, Long> latestQuotaUpdates = new ConcurrentHashMap<>();
    private Subscription eventEmitterSubscription;

    @Inject
    public DefaultEvictionOperations(V3JobOperations jobOperations,
                                     ApplicationSlaManagementService capacityGroupService,
                                     ManagementSubsystemInitializer capacityGroupServiceInitializer) { // To enforce correct initialization order
        this.jobOperations = jobOperations;
        this.capacityGroupService = capacityGroupService;

    }

    @Activator
    public void enterActiveMode() {
        // As eviction request may run concurrently at high rate, instead of reactively sending an update on each change
        // we emit all changes periodically.
        this.eventEmitterSubscription = Observable
                .interval(0, QUOTA_UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS)
                .subscribe(tick -> emitQuotaUpdates());
    }

    @PreDestroy
    public void shutdown() {
        ObservableExt.safeUnsubscribe(eventEmitterSubscription);
    }

    @Override
    public SystemDisruptionBudget getGlobalDisruptionBudget() {
        return GLOBAL_DISRUPTION_BUDGET;
    }

    @Override
    public SystemDisruptionBudget getTierDisruptionBudget(Tier tier) {
        return TIER_DISRUPTION_BUDGET;
    }

    @Override
    public SystemDisruptionBudget getCapacityGroupDisruptionBudget(String capacityGroupName) {
        return DEFAULT_CAPACITY_GROUP_DISRUPTION_BUDGET;
    }

    @Override
    public EvictionQuota getGlobalEvictionQuota() {
        return toEvictionQuota(Reference.global(), globalTokenBucket);
    }

    @Override
    public EvictionQuota getTierEvictionQuota(Tier tier) {
        return toEvictionQuota(Reference.tier(tier), getTierTokenBucket(tier));
    }

    @Override
    public EvictionQuota getCapacityGroupEvictionQuota(String capacityGroupName) {
        if (capacityGroupService.getApplicationSLA(capacityGroupName) == null) {
            throw EvictionException.capacityGroupNotFound(capacityGroupName);
        }
        return toEvictionQuota(Reference.capacityGroup(capacityGroupName), getCapacityGroupTokenBucket(capacityGroupName));
    }

    @Override
    public Completable terminateTask(String taskId, String reason) {
        return Observable
                .fromCallable(() -> {
                    EvictionContext context = resolveContext(taskId);
                    try {
                        consumeQuotasOrThrowAnException(context);
                    } catch (Exception e) {
                        eventSubject.onNext(EvictionEvent.newTaskTerminationEvent(context.getTask().getId(), false));
                        throw e;
                    }
                    return context;
                })
                .flatMap(context -> jobOperations
                        .killTask(context.getTask().getId(), false, reason)
                        .doOnCompleted(() -> eventSubject.onNext(EvictionEvent.newTaskTerminationEvent(context.getTask().getId(), true)))
                )
                .toCompletable();
    }

    @Override
    public Observable<EvictionEvent> events(boolean includeSnapshot) {
        return includeSnapshot
                ? eventObservable.compose(ObservableExt.head(this::buildSnapshot))
                : eventObservable;
    }

    private List<EvictionEvent> buildSnapshot() {
        List<EvictionEvent> snapshot = new ArrayList<>();

        snapshot.add(EvictionEvent.newSystemDisruptionBudgetEvent(GLOBAL_DISRUPTION_BUDGET));
        snapshot.add(EvictionEvent.newGlobalQuotaEvent(globalTokenBucket));
        for (Tier tier : Tier.values()) {
            snapshot.add(EvictionEvent.newSystemDisruptionBudgetEvent(
                    TIER_DISRUPTION_BUDGET.toBuilder().withReference(Reference.tier(tier)).build()
            ));
            snapshot.add(EvictionEvent.newTierQuotaEvent(tier, getTierTokenBucket(tier)));
        }
        for (ApplicationSLA capacityGroup : capacityGroupService.getApplicationSLAs()) {
            String name = capacityGroup.getAppName();
            snapshot.add(EvictionEvent.newSystemDisruptionBudgetEvent(
                    DEFAULT_CAPACITY_GROUP_DISRUPTION_BUDGET.toBuilder().withReference(Reference.capacityGroup(name)).build()
            ));
            snapshot.add(EvictionEvent.newCapacityGroupQuotaEvent(capacityGroup, getCapacityGroupTokenBucket(name)));
        }

        snapshot.add(EvictionEvent.newSnapshotEndEvent());

        return snapshot;
    }

    private EvictionContext resolveContext(String taskId) {
        Pair<Job<?>, Task> jobAndTask = checkTaskIsRunningOrThrowAnException(taskId);
        return new EvictionContext(jobAndTask.getLeft(), jobAndTask.getRight());
    }

    private TokenBucket getTierTokenBucket(Tier tier) {
        return tierTokenBuckets.computeIfAbsent(tier, t -> newTokenBucket(t.name(), TIER_DISRUPTION_BUDGET));
    }


    private TokenBucket getCapacityGroupTokenBucket(String capacityGroupName) {
        return capacityGroupTokenBuckets.computeIfAbsent(capacityGroupName, g -> newTokenBucket(capacityGroupName, DEFAULT_CAPACITY_GROUP_DISRUPTION_BUDGET));
    }

    private TokenBucket newTokenBucket(String name, SystemDisruptionBudget disruptionBudget) {
        return TokenBucketPolicies.newTokenBucket(name, disruptionBudget.getTokenBucketPolicy());
    }

    private EvictionQuota toEvictionQuota(Reference reference, TokenBucket tokenBucket) {
        return EvictionQuota.newBuilder()
                .withReference(reference)
                .withQuota(tokenBucket.getNumberOfTokens())
                .build();
    }

    private Pair<Job<?>, Task> checkTaskIsRunningOrThrowAnException(String taskId) {
        Optional<Pair<Job<?>, Task>> jobAndTask = jobOperations.findTaskById(taskId);
        if (!jobAndTask.isPresent()) {
            throw EvictionException.taskNotFound(taskId);
        }
        Task task = jobAndTask.get().getRight();
        TaskState state = task.getStatus().getState();
        if (state == TaskState.Accepted) {
            throw EvictionException.taskNotScheduledYet(task);
        }
        if (!TaskState.isBefore(state, TaskState.KillInitiated)) {
            throw EvictionException.taskAlreadyStopped(task);
        }
        return jobAndTask.get();
    }

    private void consumeQuotasOrThrowAnException(EvictionContext context) {
        synchronized (quotaLock) {
            // Check quota availability at all levels.
            checkGlobalQuotaOrThrowAnException();
            checkTierQuotaOrThrowAnException(context);
            checkCapacityGroupQuotaOrThrowAnException(context);
            checkJobDisruptionBudgetQuotaOrThrowAnException(context);

            // Now consume it.
            consumeGlobalQuota();
            consumeTierQuota(context);
            consumeCapacityGroupQuota(context);
        }
    }

    private void emitQuotaUpdates() {
        if (globalTokenBucket.getNumberOfTokens() != latestQuotaUpdates.getOrDefault(Reference.global(), 0L)) {
            eventSubject.onNext(EvictionEvent.newGlobalQuotaEvent(globalTokenBucket));
            latestQuotaUpdates.put(Reference.global(), globalTokenBucket.getNumberOfTokens());
        }
        for (Tier tier : Tier.values()) {
            TokenBucket tokenBucket = getTierTokenBucket(tier);
            Reference reference = Reference.tier(tier);
            if (tokenBucket.getNumberOfTokens() != latestQuotaUpdates.getOrDefault(reference, 0L)) {
                eventSubject.onNext(EvictionEvent.newTierQuotaEvent(tier, tokenBucket));
                latestQuotaUpdates.put(reference, tokenBucket.getNumberOfTokens());
            }
        }

        for (ApplicationSLA capacityGroup : capacityGroupService.getApplicationSLAs()) {
            TokenBucket tokenBucket = getCapacityGroupTokenBucket(capacityGroup.getAppName());
            Reference reference = Reference.capacityGroup(capacityGroup.getAppName());
            if (tokenBucket.getNumberOfTokens() != latestQuotaUpdates.getOrDefault(reference, 0L)) {
                eventSubject.onNext(EvictionEvent.newCapacityGroupQuotaEvent(capacityGroup, tokenBucket));
                latestQuotaUpdates.put(reference, tokenBucket.getNumberOfTokens());
            }
        }
    }

    private void checkGlobalQuotaOrThrowAnException() {
        if (globalTokenBucket.getNumberOfTokens() <= 0) {
            throw EvictionException.noAvailableGlobalQuota();
        }
    }

    private void consumeGlobalQuota() {
        globalTokenBucket.tryTake();
    }

    private void checkTierQuotaOrThrowAnException(EvictionContext context) {
        Tier tier = context.getCapacityGroup().getTier();
        if (getTierTokenBucket(tier).getNumberOfTokens() <= 0) {
            throw EvictionException.noAvailableTierQuota(tier);
        }
    }

    private void consumeTierQuota(EvictionContext context) {
        getTierTokenBucket(context.getCapacityGroup().getTier()).tryTake();
    }

    private void checkCapacityGroupQuotaOrThrowAnException(EvictionContext context) {
        ApplicationSLA capacityGroup = context.getCapacityGroup();
        if (getCapacityGroupTokenBucket(capacityGroup.getAppName()).getNumberOfTokens() <= 0) {
            throw EvictionException.noAvailableCapacityGroupQuota(capacityGroup.getAppName());
        }
    }

    private void consumeCapacityGroupQuota(EvictionContext context) {
        getCapacityGroupTokenBucket(context.getCapacityGroup().getAppName()).tryTake();
    }

    /**
     * Simplistic implementation, until proper job disruption budget is defined. Allow task eviction only if
     * all desired tasks are in Started state.
     */
    private void checkJobDisruptionBudgetQuotaOrThrowAnException(EvictionContext context) {
        if (context.getTasksDesired() > context.getTasksStarted()) {
            throw EvictionException.noAvailableJobQuota(context.getJob(), context.getTasksDesired(), context.getTasksStarted());
        }
    }

    private class EvictionContext {
        private final Job<?> job;
        private final Task task;
        private final List<Task> allTasks;

        private final ApplicationSLA capacityGroup;

        private final int tasksDesired;
        private final int tasksStarted;

        private EvictionContext(Job<?> job, Task task) {
            this.job = job;
            this.task = task;
            this.allTasks = jobOperations.getTasks(job.getId());
            this.capacityGroup = getCapacityGroup(job);

            this.tasksDesired = JobFunctions.isServiceJob(job)
                    ? ((ServiceJobExt) job.getJobDescriptor().getExtensions()).getCapacity().getDesired()
                    : ((BatchJobExt) job.getJobDescriptor().getExtensions()).getSize();

            this.tasksStarted = (int) allTasks.stream().filter(t -> t.getStatus().getState() == TaskState.Started).count();
        }

        private Job<?> getJob() {
            return job;
        }

        private Task getTask() {
            return task;
        }

        private List<Task> getAllTasks() {
            return allTasks;
        }

        private ApplicationSLA getCapacityGroup() {
            return capacityGroup;
        }

        private int getTasksDesired() {
            return tasksDesired;
        }

        private int getTasksStarted() {
            return tasksStarted;
        }

        private ApplicationSLA getCapacityGroup(Job<?> job) {
            ApplicationSLA result = capacityGroupService.getApplicationSLA(job.getJobDescriptor().getCapacityGroup());
            if (result != null) {
                return result;
            }
            result = capacityGroupService.getApplicationSLA(job.getJobDescriptor().getApplicationName());
            if (result != null) {
                return result;
            }
            return capacityGroupService.getApplicationSLA(ApplicationSlaManagementService.DEFAULT_APPLICATION);
        }
    }
}
