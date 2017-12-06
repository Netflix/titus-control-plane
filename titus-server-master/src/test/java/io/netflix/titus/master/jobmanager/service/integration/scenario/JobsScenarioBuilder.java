package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.DefaultV3JobOperations;
import io.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import io.netflix.titus.master.jobmanager.service.batch.BatchDifferenceResolver;
import io.netflix.titus.master.jobmanager.service.integration.scenario.JobScenarioBuilder.EventHolder;
import io.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import io.netflix.titus.master.jobmanager.service.service.ServiceDifferenceResolver;
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

    public JobsScenarioBuilder() {
        when(configuration.getReconcilerActiveTimeoutMs()).thenReturn(RECONCILER_ACTIVE_TIMEOUT_MS);
        when(configuration.getReconcilerIdleTimeoutMs()).thenReturn(RECONCILER_IDLE_TIMEOUT_MS);

        when(configuration.getTaskInLaunchedStateTimeoutMs()).thenReturn(LAUNCHED_TIMEOUT_MS);
        when(configuration.getBatchTaskInStartInitiatedStateTimeoutMs()).thenReturn(START_INITIATED_TIMEOUT_MS);
        when(configuration.getTaskInKillInitiatedStateTimeoutMs()).thenReturn(KILL_INITIATED_TIMEOUT_MS);
        when(configuration.getTaskKillAttempts()).thenReturn(2L);

        jobStore.events().subscribe(storeEvents);

        BatchDifferenceResolver batchDifferenceResolver = new BatchDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore,
                Clocks.testScheduler(testScheduler),
                testScheduler
        );
        ServiceDifferenceResolver serviceDifferenceResolver = new ServiceDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore,
                testScheduler
        );
        this.jobOperations = new DefaultV3JobOperations(
                configuration,
                batchDifferenceResolver,
                serviceDifferenceResolver,
                jobStore,
                schedulingService,
                vmService,
                capacityGroupService,
                testScheduler
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

    public JobsScenarioBuilder scheduleBatchJob(JobDescriptor<BatchJobExt> jobDescriptor,
                                                Function<JobScenarioBuilder<BatchJobExt>, JobScenarioBuilder<BatchJobExt>> jobScenario) {

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

        JobScenarioBuilder<BatchJobExt> jobScenarioBuilder = new JobScenarioBuilder<>(jobId, jobEventsSubscriber, storeEventsSubscriber, jobOperations, schedulingService, jobStore, vmService, testScheduler);
        jobScenarioBuilders.add(jobScenarioBuilder);
        jobScenario.apply(jobScenarioBuilder);
        return this;
    }
}
