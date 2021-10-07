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

package com.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor.JobDescriptorExt;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobAssertions;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.sanitizer.EntitySanitizerBuilder;
import com.netflix.titus.common.model.sanitizer.VerifierMode;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.jobmanager.service.DefaultV3JobOperations;
import com.netflix.titus.master.jobmanager.service.JobManagerConfiguration;
import com.netflix.titus.master.jobmanager.service.JobReconciliationFrameworkFactory;
import com.netflix.titus.master.jobmanager.service.JobServiceRuntime;
import com.netflix.titus.master.jobmanager.service.VersionSupplier;
import com.netflix.titus.master.jobmanager.service.VersionSuppliers;
import com.netflix.titus.master.jobmanager.service.batch.BatchDifferenceResolver;
import com.netflix.titus.master.jobmanager.service.integration.scenario.StubbedJobStore.StoreEvent;
import com.netflix.titus.master.jobmanager.service.limiter.JobSubmitLimiter;
import com.netflix.titus.master.jobmanager.service.service.ServiceDifferenceResolver;
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
    public static final int CONCURRENT_STORE_UPDATE_LIMIT = 5;

    public static final long MIN_RETRY_INTERVAL_MS = 10;

    public static final long LAUNCHED_TIMEOUT_MS = 5_000;
    public static final long START_INITIATED_TIMEOUT_MS = 10_000;
    public static final long KILL_INITIATED_TIMEOUT_MS = 30_000;

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private final JobManagerConfiguration configuration = mock(JobManagerConfiguration.class);
    private final FeatureActivationConfiguration featureActivationConfiguration = mock(FeatureActivationConfiguration.class);
    private final JobConfiguration jobSanitizerConfiguration = Archaius2Ext.newConfiguration(JobConfiguration.class);
    private final ApplicationSlaManagementService capacityGroupService = new StubbedApplicationSlaManagementService();
    private final StubbedJobStore jobStore = new StubbedJobStore();
    private final VersionSupplier versionSupplier;
    private final JobServiceRuntime runtime;

    private volatile int concurrentStoreUpdateLimit = CONCURRENT_STORE_UPDATE_LIMIT;

    private DefaultV3JobOperations jobOperations;

    private final ExtTestSubscriber<Pair<StoreEvent, ?>> storeEvents = new ExtTestSubscriber<>();

    private final List<JobScenarioBuilder> jobScenarioBuilders = new ArrayList<>();

    public JobsScenarioBuilder() {
        this.versionSupplier = VersionSuppliers.newInstance(titusRuntime.getClock());
        when(configuration.getReconcilerActiveTimeoutMs()).thenReturn(RECONCILER_ACTIVE_TIMEOUT_MS);
        when(configuration.getReconcilerIdleTimeoutMs()).thenReturn(RECONCILER_IDLE_TIMEOUT_MS);

        when(configuration.getMaxActiveJobs()).thenReturn(20_000L);
        when(configuration.getActiveNotStartedTasksLimit()).thenReturn(ACTIVE_NOT_STARTED_TASKS_LIMIT);
        when(configuration.getConcurrentReconcilerStoreUpdateLimit()).thenAnswer(invocation -> concurrentStoreUpdateLimit);
        when(configuration.getTaskInLaunchedStateTimeoutMs()).thenReturn(LAUNCHED_TIMEOUT_MS);
        when(configuration.getBatchTaskInStartInitiatedStateTimeoutMs()).thenReturn(START_INITIATED_TIMEOUT_MS);
        when(configuration.getTaskInKillInitiatedStateTimeoutMs()).thenReturn(KILL_INITIATED_TIMEOUT_MS);
        when(configuration.getMinRetryIntervalMs()).thenReturn(MIN_RETRY_INTERVAL_MS);
        when(configuration.getTaskRetryerResetTimeMs()).thenReturn(TimeUnit.MINUTES.toMillis(5));
        when(configuration.getTaskKillAttempts()).thenReturn(2L);
        when(featureActivationConfiguration.isMoveTaskValidationEnabled()).thenReturn(true);
        when(featureActivationConfiguration.isOpportunisticResourcesSchedulingEnabled()).thenReturn(true);

        jobStore.events().subscribe(storeEvents);

        this.runtime = new JobServiceRuntime(
                configuration,
                new StubbedComputeProvider(),
                titusRuntime
        );

        this.jobOperations = createAndActivateV3JobOperations();
    }

    private DefaultV3JobOperations createAndActivateV3JobOperations() {
        TokenBucket stuckInStateRateLimiter = Limiters.unlimited("stuckInState");
        BatchDifferenceResolver batchDifferenceResolver = new BatchDifferenceResolver(
                configuration,
                runtime,
                featureActivationConfiguration,
                capacityGroupService,
                jobStore,
                versionSupplier,
                stuckInStateRateLimiter,
                titusRuntime,
                testScheduler
        );
        ServiceDifferenceResolver serviceDifferenceResolver = new ServiceDifferenceResolver(
                configuration,
                runtime,
                featureActivationConfiguration,
                capacityGroupService,
                jobStore,
                versionSupplier,
                stuckInStateRateLimiter,
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
                featureActivationConfiguration,
                jobStore,
                runtime,
                new JobReconciliationFrameworkFactory(
                        configuration,
                        featureActivationConfiguration,
                        batchDifferenceResolver,
                        serviceDifferenceResolver,
                        jobStore,
                        capacityGroupService,
                        newJobSanitizer(VerifierMode.Permissive),
                        newJobSanitizer(VerifierMode.Strict),
                        versionSupplier,
                        titusRuntime,
                        Optional.of(testScheduler)
                ),
                jobSubmitLimiter,
                new ManagementSubsystemInitializer(null, null),
                titusRuntime,
                EntitySanitizerBuilder.stdBuilder().build(),
                versionSupplier
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

            JobScenarioBuilder jobScenarioBuilder = new JobScenarioBuilder(
                    job.getId(),
                    jobEventsSubscriber,
                    storeEventsSubscriber,
                    jobOperations,
                    jobStore,
                    (StubbedComputeProvider) runtime.getComputeProvider(),
                    versionSupplier,
                    titusRuntime,
                    testScheduler
            );
            jobScenarioBuilders.add(jobScenarioBuilder);
        });

        return this;
    }

    public JobsScenarioBuilder withConcurrentStoreUpdateLimit(int concurrentStoreUpdateLimit) {
        this.concurrentStoreUpdateLimit = concurrentStoreUpdateLimit;
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

    public JobScenarioBuilder getJobScenario(int idx) {
        return jobScenarioBuilders.get(idx);
    }

    public List<JobScenarioBuilder> getJobScenarios() {
        return jobScenarioBuilders;
    }

    public <E extends JobDescriptorExt> JobsScenarioBuilder inJob(int idx, Function<JobScenarioBuilder, JobScenarioBuilder> jobScenario) {
        JobScenarioBuilder jobScenarioBuilder = getJobScenario(idx);
        if (jobScenarioBuilder == null) {
            throw new IllegalArgumentException(String.format("No job with index %s registered", idx));
        }
        jobScenario.apply(jobScenarioBuilder);
        return this;
    }

    public <E extends JobDescriptorExt> JobsScenarioBuilder scheduleJob(JobDescriptor<E> jobDescriptor,
                                                                        Function<JobScenarioBuilder, JobScenarioBuilder> jobScenario) {

        JobScenarioBuilder.EventHolder<JobManagerEvent<?>> jobEventsSubscriber = new JobScenarioBuilder.EventHolder<>(jobStore);
        JobScenarioBuilder.EventHolder<Pair<StoreEvent, ?>> storeEventsSubscriber = new JobScenarioBuilder.EventHolder<>(jobStore);
        AtomicReference<String> jobIdRef = new AtomicReference<>();

        jobOperations.createJob(jobDescriptor, CallMetadata.newBuilder().withCallerId("Testing").withCallReason("Testing job creation").build()).doOnNext(jobId -> {
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

        JobScenarioBuilder jobScenarioBuilder = new JobScenarioBuilder(
                jobId,
                jobEventsSubscriber,
                storeEventsSubscriber,
                jobOperations,
                jobStore,
                (StubbedComputeProvider) runtime.getComputeProvider(),
                versionSupplier,
                titusRuntime,
                testScheduler
        );
        jobScenarioBuilders.add(jobScenarioBuilder);
        jobScenario.apply(jobScenarioBuilder);
        jobScenarioBuilder.expectVersionsOrdered();
        return this;
    }

    private EntitySanitizer newJobSanitizer(VerifierMode verifierMode) {
        return new JobSanitizerBuilder()
                .withVerifierMode(verifierMode)
                .withJobConstraintConfiguration(jobSanitizerConfiguration)
                .withJobAsserts(new JobAssertions(
                        jobSanitizerConfiguration,
                        instanceType -> ResourceDimension.newBuilder()
                                .withCpus(64)
                                .withGpu(8)
                                .withMemoryMB(256 * 1024)
                                .withDiskMB(1024 * 1024)
                                .withNetworkMbs(10 * 1024)
                                .build()

                ))
                .build();
    }
}
