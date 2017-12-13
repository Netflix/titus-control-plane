package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.fenzo.ConstraintEvaluator;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.time.TestClock;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.DefaultV3JobOperations;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.JobReconciliationFrameworkFactory;
import io.netflix.titus.master.jobmanager.service.batch.BatchDifferenceResolver;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder.EventHolder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import io.netflix.titus.master.jobmanager.service.service.ServiceDifferenceResolver;
import io.netflix.titus.master.scheduler.ConstraintEvaluatorTransformer;
import io.netflix.titus.master.scheduler.constraint.GlobalConstraintEvaluator;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobsScenarioBuilder {

    public static final long RECONCILER_ACTIVE_TIMEOUT_MS = 50L;
    public static final long RECONCILER_IDLE_TIMEOUT_MS = 50;

    public static final int ACTIVE_NOT_STARTED_TASKS_LIMIT = 5;

    public static final long LAUNCHED_TIMEOUT_MS = 5_000;
    public static final long START_INITIATED_TIMEOUT_MS = 10_000;
    public static final long KILL_INITIATED_TIMEOUT_MS = 30_000;

    private final TestScheduler testScheduler = Schedulers.test();

    private final JobManagerConfiguration configuration = mock(JobManagerConfiguration.class);
    private final ApplicationSlaManagementService capacityGroupService = new StubbedApplicationSlaManagementService();
    private final StubbedSchedulingService schedulingService = new StubbedSchedulingService();
    private final StubbedVirtualMachineMasterService vmService = new StubbedVirtualMachineMasterService();
    private final StubbedJobStore jobStore = new StubbedJobStore();

    private final DefaultV3JobOperations jobOperations;

    private final ExtTestSubscriber<Pair<StoreEvent, ?>> storeEvents = new ExtTestSubscriber<>();

    private final List<JobScenarioBuilder<?>> jobScenarioBuilders = new ArrayList<>();

    private final ConstraintEvaluatorTransformer<Pair<String, String>> constraintEvaluatorTransformer = null;

    public JobsScenarioBuilder() {
        when(configuration.getReconcilerActiveTimeoutMs()).thenReturn(RECONCILER_ACTIVE_TIMEOUT_MS);
        when(configuration.getReconcilerIdleTimeoutMs()).thenReturn(RECONCILER_IDLE_TIMEOUT_MS);

        when(configuration.getActiveNotStartedTasksLimit()).thenReturn(ACTIVE_NOT_STARTED_TASKS_LIMIT);
        when(configuration.getTaskInLaunchedStateTimeoutMs()).thenReturn(LAUNCHED_TIMEOUT_MS);
        when(configuration.getBatchTaskInStartInitiatedStateTimeoutMs()).thenReturn(START_INITIATED_TIMEOUT_MS);
        when(configuration.getTaskInKillInitiatedStateTimeoutMs()).thenReturn(KILL_INITIATED_TIMEOUT_MS);
        when(configuration.getTaskRetryerResetTimeMs()).thenReturn(TimeUnit.MINUTES.toMillis(5));
        when(configuration.getTaskKillAttempts()).thenReturn(2L);

        jobStore.events().subscribe(storeEvents);

        TestClock clock = Clocks.testScheduler(testScheduler);

        GlobalConstraintEvaluator globalConstraintEvaluator = (taskRequest, targetVM, taskTrackerState) ->
                new ConstraintEvaluator.Result(true, null);

        BatchDifferenceResolver batchDifferenceResolver = new BatchDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore,
                constraintEvaluatorTransformer,
                globalConstraintEvaluator,
                clock,
                testScheduler
        );
        ServiceDifferenceResolver serviceDifferenceResolver = new ServiceDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore,
                constraintEvaluatorTransformer,
                globalConstraintEvaluator,
                clock,
                testScheduler
        );
        this.jobOperations = new DefaultV3JobOperations(
                configuration,
                jobStore,
                vmService,
                new JobReconciliationFrameworkFactory(
                        configuration,
                        batchDifferenceResolver,
                        serviceDifferenceResolver,
                        jobStore,
                        schedulingService,
                        capacityGroupService,
                        globalConstraintEvaluator,
                        constraintEvaluatorTransformer,
                        testScheduler
                )
        );
        jobOperations.enterActiveMode();
    }

    public JobsScenarioBuilder trigger() {
        testScheduler.triggerActions();
        return this;
    }

    public JobsScenarioBuilder advance() {
        testScheduler.advanceTimeBy(JobsScenarioBuilder.RECONCILER_ACTIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        return this;
    }

    public <E extends JobDescriptorExt> JobScenarioBuilder<E> getJobScenario(int idx) {
        return (JobScenarioBuilder<E>) jobScenarioBuilders.get(idx);
    }

    public <E extends JobDescriptorExt> JobsScenarioBuilder scheduleJob(JobDescriptor<E> jobDescriptor,
                                                                        Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> jobScenario) {

        EventHolder<JobManagerEvent<?>> jobEventsSubscriber = new EventHolder<>(jobStore);
        EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber = new EventHolder<>(jobStore);
        AtomicReference<String> jobIdRef = new AtomicReference<>();

        jobOperations.createJob(jobDescriptor).doOnNext(jobId -> {
            jobOperations.observeJob(jobId).subscribe(jobEventsSubscriber);
            jobStore.events(jobId).subscribe(storeEventsSubscriber);
        }).subscribe(jobIdRef::set);

        trigger();

        String jobId = jobIdRef.get();
        assertThat(jobId).describedAs("Job not created").isNotNull();

        JobScenarioBuilder<E> jobScenarioBuilder = new JobScenarioBuilder<>(jobId, jobEventsSubscriber, storeEventsSubscriber, jobOperations, schedulingService, jobStore, vmService, testScheduler);
        jobScenarioBuilders.add(jobScenarioBuilder);
        jobScenario.apply(jobScenarioBuilder);
        return this;
    }
}
