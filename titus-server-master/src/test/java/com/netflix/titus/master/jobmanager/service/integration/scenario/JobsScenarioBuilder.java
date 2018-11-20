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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.VerifierMode;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.DefaultV3JobOperations;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.master.jobmanager.service.JobReconciliationFrameworkFactory;
import com.netflix.titus.master.jobmanager.service.batch.BatchDifferenceResolver;
import com.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.master.jobmanager.service.service.ServiceDifferenceResolver;
import com.netflix.titus.master.scheduler.constraint.ConstraintEvaluatorTransformer;
import com.netflix.titus.master.scheduler.constraint.SystemHardConstraint;
import com.netflix.titus.master.scheduler.constraint.SystemSoftConstraint;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.ManagementSubsystemInitializer;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.jayway.awaitility.Awaitility.await;
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

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private final JobManagerConfiguration configuration = mock(JobManagerConfiguration.class);
    private final JobConfiguration jobSanitizerConfiguration = mock(JobConfiguration.class);
    private final ApplicationSlaManagementService capacityGroupService = new StubbedApplicationSlaManagementService();
    private final StubbedSchedulingService schedulingService = new StubbedSchedulingService();
    private final StubbedVirtualMachineMasterService vmService = new StubbedVirtualMachineMasterService();
    private final StubbedJobStore jobStore = new StubbedJobStore();

    private DefaultV3JobOperations jobOperations;

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

        this.jobOperations = createAndActivateV3JobOperations();
    }

    private DefaultV3JobOperations createAndActivateV3JobOperations() {
        SystemSoftConstraint systemSoftConstraint = new SystemSoftConstraint() {
            @Override
            public String getName() {
                return "Test System Soft Constraint";
            }

            @Override
            public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                return 1.0;
            }
        };
        SystemHardConstraint systemHardConstraint = new SystemHardConstraint() {
            @Override
            public String getName() {
                return "TestSystemHardConstraint";
            }

            @Override
            public Result evaluate(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
                return new ConstraintEvaluator.Result(true, "");
            }
        };

        BatchDifferenceResolver batchDifferenceResolver = new BatchDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore,
                constraintEvaluatorTransformer,
                systemSoftConstraint,
                systemHardConstraint,
                titusRuntime,
                testScheduler
        );
        ServiceDifferenceResolver serviceDifferenceResolver = new ServiceDifferenceResolver(
                configuration,
                capacityGroupService,
                schedulingService,
                vmService,
                jobStore,
                constraintEvaluatorTransformer,
                systemSoftConstraint,
                systemHardConstraint,
                titusRuntime,
                testScheduler
        );


        JobSubmitLimiter jobSubmitLimiter = new JobSubmitLimiter() {
            @Override
            public <JOB_DESCR> Optional<String> checkIfAllowed(JOB_DESCR jobDescriptor) {
                return Optional.empty();
            }

            @Override
            public <JOB_DESCR> Optional<String> reserveId(JOB_DESCR jobDescriptor) {
                return Optional.empty();
            }

            @Override
            public <JOB_DESCR> void releaseId(JOB_DESCR jobDescriptor) {
            }
        };

        DefaultV3JobOperations v3JobOperations = new DefaultV3JobOperations(
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
                        systemSoftConstraint,
                        systemHardConstraint,
                        constraintEvaluatorTransformer,
                        newJobSanitizer(VerifierMode.Permissive),
                        newJobSanitizer(VerifierMode.Strict),
                        titusRuntime,
                        Optional.of(testScheduler)
                ),
                jobSubmitLimiter,
                new ManagementSubsystemInitializer(null, null),
                titusRuntime
        );
        v3JobOperations.enterActiveMode();

        return v3JobOperations;
    }

    public DefaultV3JobOperations getJobOperations() {
        return jobOperations;
    }

    public TestScheduler getTestScheduler() {
        return testScheduler;
    }

    public TitusRuntime getTitusRuntime() {
        return titusRuntime;
    }

    public JobsScenarioBuilder reboot() {
        this.jobOperations.shutdown();
        this.jobScenarioBuilders.clear();

        this.jobOperations = createAndActivateV3JobOperations();

        jobOperations.getJobs().forEach(job -> {
            JobScenarioBuilder.EventHolder<JobManagerEvent<?>> jobEventsSubscriber = new JobScenarioBuilder.EventHolder<>(jobStore);
            JobScenarioBuilder.EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber = new JobScenarioBuilder.EventHolder<>(jobStore);

            jobOperations.observeJob(job.getId()).subscribe(jobEventsSubscriber);
            jobStore.events(job.getId()).subscribe(storeEventsSubscriber);

            JobScenarioBuilder<?> jobScenarioBuilder = new JobScenarioBuilder<>(job.getId(), jobEventsSubscriber, storeEventsSubscriber, jobOperations, schedulingService, jobStore, vmService, titusRuntime, testScheduler);
            jobScenarioBuilders.add(jobScenarioBuilder);
        });

        return this;
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

    public List<JobScenarioBuilder<?>> getJobScenarios() {
        return jobScenarioBuilders;
    }

    public <E extends JobDescriptorExt> JobsScenarioBuilder inJob(int idx, Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> jobScenario) {
        JobScenarioBuilder<E> jobScenarioBuilder = getJobScenario(idx);
        if (jobScenarioBuilder == null) {
            throw new IllegalArgumentException(String.format("No job with index %s registered", idx));
        }
        jobScenario.apply(jobScenarioBuilder);
        return this;
    }

    public <E extends JobDescriptorExt> JobsScenarioBuilder scheduleJob(JobDescriptor<E> jobDescriptor,
                                                                        Function<JobScenarioBuilder<E>, JobScenarioBuilder<E>> jobScenario) {

        JobScenarioBuilder.EventHolder<JobManagerEvent<?>> jobEventsSubscriber = new JobScenarioBuilder.EventHolder<>(jobStore);
        JobScenarioBuilder.EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber = new JobScenarioBuilder.EventHolder<>(jobStore);
        AtomicReference<String> jobIdRef = new AtomicReference<>();

        jobOperations.createJob(jobDescriptor).doOnNext(jobId -> {
            jobOperations.observeJob(jobId).subscribe(jobEventsSubscriber);
            jobStore.events(jobId).subscribe(storeEventsSubscriber);
        }).subscribe(jobIdRef::set);

        trigger();

        await().timeout(5, TimeUnit.SECONDS).until(() -> {
            if (jobIdRef.get() != null) {
                return true;
            }
            advance();
            return false;
        });
        String jobId = jobIdRef.get();
        assertThat(jobId).describedAs("Job not created").isNotNull();

        JobScenarioBuilder<E> jobScenarioBuilder = new JobScenarioBuilder<>(jobId, jobEventsSubscriber, storeEventsSubscriber, jobOperations, schedulingService, jobStore, vmService, titusRuntime, testScheduler);
        jobScenarioBuilders.add(jobScenarioBuilder);
        jobScenario.apply(jobScenarioBuilder);
        return this;
    }

    private EntitySanitizer newJobSanitizer(VerifierMode verifierMode) {
        return new JobSanitizerBuilder()
                .withVerifierMode(verifierMode)
                .withJobConstraintConfiguration(jobSanitizerConfiguration)
                .withMaxContainerSizeResolver(instanceType -> ResourceDimension.newBuilder()
                        .withCpus(64)
                        .withGpu(8)
                        .withMemoryMB(256 * 1024)
                        .withDiskMB(1024 * 1024)
                        .withNetworkMbs(10 * 1024)
                        .build()
                )
                .build();
    }
}
