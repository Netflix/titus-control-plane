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

package com.netflix.titus.master.appscale.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.appscale.model.AutoScalableTarget;
import com.netflix.titus.api.appscale.model.AutoScalingPolicy;
import com.netflix.titus.api.appscale.model.PolicyType;
import com.netflix.titus.api.appscale.service.AutoScalePolicyException;
import com.netflix.titus.api.jobmanager.model.job.Capacity;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import com.netflix.titus.api.jobmanager.model.job.JobModel;
import com.netflix.titus.api.jobmanager.model.job.JobState;
import com.netflix.titus.api.jobmanager.model.job.event.JobManagerEvent;
import com.netflix.titus.api.jobmanager.model.job.event.JobUpdateEvent;
import com.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.api.model.event.JobStateChangeEvent;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import com.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import com.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.model.v2.parameter.Parameters;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2StageMetadata;
import com.netflix.titus.common.util.rx.eventbus.RxEventBus;
import com.netflix.titus.common.util.rx.eventbus.internal.DefaultRxEventBus;
import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.job.V2JobOperations;
import com.netflix.titus.master.job.service.ServiceJobMgr;
import com.netflix.titus.runtime.store.v3.memory.InMemoryPolicyStore;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;
import rx.schedulers.Schedulers;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DefaultAppScaleManagerTest {
    private static Logger log = LoggerFactory.getLogger(DefaultAppScaleManagerTest.class);

    @Test
    public void checkTargetTrackingPolicy() throws Exception {
        checkCreatePolicyFlow(PolicyType.TargetTrackingScaling);
    }

    @Test
    public void checkStepCreatePolicyFlow() throws Exception {
        checkCreatePolicyFlow(PolicyType.StepScaling);
    }

    private void checkCreatePolicyFlow(PolicyType policyType) throws Exception {
        // create instance of DefaultAppScaleManager
        AutoScalingPolicyTests.MockAlarmClient mockAlarmClient = new AutoScalingPolicyTests.MockAlarmClient();
        AutoScalingPolicyTests.MockAppAutoScalingClient mockAppAutoScalingClient = new AutoScalingPolicyTests.MockAppAutoScalingClient();
        InMemoryPolicyStore policyStore = new InMemoryPolicyStore();
        String jobIdOne = UUID.randomUUID().toString();
        String jobIdTwo = UUID.randomUUID().toString();
        V3JobOperations v3JobOperations = mockV3Operations(jobIdOne, jobIdTwo);

        DefaultAppScaleManager appScaleManager = new DefaultAppScaleManager(policyStore, mockAlarmClient, mockAppAutoScalingClient,
                v3JobOperations, new DefaultRegistry(),
                AutoScalingPolicyTests.mockAppScaleManagerConfiguration(), Schedulers.immediate());

        AutoScalingPolicy autoScalingPolicyOne;
        AutoScalingPolicy autoScalingPolicyTwo;
        if (policyType == PolicyType.StepScaling) {
            autoScalingPolicyOne = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdOne);
            autoScalingPolicyTwo = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdTwo);
        } else {
            autoScalingPolicyOne = AutoScalingPolicyTests.buildTargetTrackingPolicy(jobIdOne);
            autoScalingPolicyTwo = AutoScalingPolicyTests.buildTargetTrackingPolicy(jobIdTwo);
        }

        // call - createAutoScalingPolicy
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyOne).toBlocking().single();
        String policyRefIdTwo = appScaleManager.createAutoScalingPolicy(autoScalingPolicyTwo).toBlocking().single();


        AutoScalingPolicyTests.waitForCondition(() -> {
            List<AutoScalingPolicy> policies = policyStore.retrievePolicies(false).toList().toBlocking().first();
            return policies.size() == 2 && mockAppAutoScalingClient.getNumPolicies() == 2
                    && mockAppAutoScalingClient.getNumScalableTargets() == 2;
        });

        // verify counts in CloudAlarmClient, AppAutoScaleClient and AppScalePolicyStore
        List<AutoScalingPolicy> policiesStored = policyStore.retrievePolicies(false).toList().toBlocking().first();
        Assertions.assertThat(policiesStored.size()).isEqualTo(2);
        Assertions.assertThat(mockAppAutoScalingClient.getNumPolicies()).isEqualTo(2);
        Assertions.assertThat(mockAppAutoScalingClient.getNumScalableTargets()).isEqualTo(2);
        if (policyType == PolicyType.StepScaling) {
            Assertions.assertThat(mockAlarmClient.getNumOfAlarmsCreated()).isEqualTo(2);
        }

        appScaleManager.removeAutoScalingPolicy(policyRefIdTwo).await();

        AutoScalingPolicyTests.waitForCondition(() -> {
            List<AutoScalingPolicy> policies = policyStore.retrievePolicies(false).toList().toBlocking().first();
            return policies.size() == 1 && mockAppAutoScalingClient.getNumPolicies() == 1
                    && mockAppAutoScalingClient.getNumScalableTargets() == 1;
        });

        // verify counts in CloudAlarmClient, AppAutoScaleClient and AppScalePolicyStore
        policiesStored = policyStore.retrievePolicies(false).toList().toBlocking().first();
        Assertions.assertThat(policiesStored.size()).isEqualTo(1);
        Assertions.assertThat(mockAppAutoScalingClient.getNumPolicies()).isEqualTo(1);
        Assertions.assertThat(mockAppAutoScalingClient.getNumScalableTargets()).isEqualTo(1);
        if (policyType == PolicyType.StepScaling) {
            Assertions.assertThat(mockAlarmClient.getNumOfAlarmsCreated()).isEqualTo(1);
        }
    }

    @Test
    public void checkV3LiveStreamTargetUpdates() throws Exception {
        String jobIdOne = UUID.randomUUID().toString();
        String jobIdTwo = UUID.randomUUID().toString();

        InMemoryPolicyStore policyStore = new InMemoryPolicyStore();
        V3JobOperations v3JobOperations = mockV3Operations(jobIdOne, jobIdTwo);
        AppScaleClientWithScalingPolicyConstraints appScalingClient = new AppScaleClientWithScalingPolicyConstraints();
        DefaultAppScaleManager appScaleManager = new DefaultAppScaleManager(policyStore,
                new AutoScalingPolicyTests.MockAlarmClient(),
                appScalingClient,
                v3JobOperations, new DefaultRegistry(),
                AutoScalingPolicyTests.mockAppScaleManagerConfiguration(), Schedulers.immediate());

        List<String> refIds = submitTwoJobs(appScaleManager, jobIdOne, jobIdTwo, policyStore);
        Assertions.assertThat(refIds.size()).isEqualTo(2);

        CountDownLatch latch = new CountDownLatch(1);
        Observable<String> jobIdTargetUpdates = appScaleManager.v3LiveStreamTargetUpdates();

        List<String> targetsUpdated = new ArrayList<>();
        jobIdTargetUpdates.subscribe(targetUpdated -> {
                    log.info("Got ScalableTarget to be updated {}", targetUpdated);
                    Assertions.assertThat(targetUpdated).isEqualTo(jobIdTwo);
                    targetsUpdated.add(targetUpdated);
                    latch.countDown();
                },
                e -> log.error("Error in v2 live stream for scalable target update", e),
                () -> log.info("Completed"));

        latch.await(60, TimeUnit.SECONDS);

        AutoScalingPolicyTests.waitForCondition(() -> {
            JobScalingConstraints jpc = appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdTwo);
            return jpc != null && jpc.getMinCapacity() == 5 && jpc.getMaxCapacity() == 15;
        });

        Assertions.assertThat(targetsUpdated.size()).isEqualTo(1);
        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdOne).getMinCapacity()).isEqualTo(1);
        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdOne).getMaxCapacity()).isEqualTo(10);
        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdTwo).getMinCapacity()).isEqualTo(5);
        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdTwo).getMaxCapacity()).isEqualTo(15);
    }

    @Test
    public void checkASGNameBuildingV3() {

        JobGroupInfo jobGroupInfoOne = JobGroupInfo.newBuilder()
                .withDetail("^1.0.0")
                .withSequence("v001")
                .withStack("main")
                .build();


        JobDescriptor<JobDescriptor.JobDescriptorExt> jobDescriptorOne = JobDescriptor.newBuilder()
                .withApplicationName("testapp")
                .withJobGroupInfo(jobGroupInfoOne).build();
        String autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV3(jobDescriptorOne);
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-main-^1.0.0-v001");


        JobGroupInfo jobGroupInfoTwo = JobGroupInfo.newBuilder()
                .withDetail("^1.0.0")
                .withStack("main")
                .build();


        JobDescriptor<JobDescriptor.JobDescriptorExt> jobDescriptorTwo = JobDescriptor.newBuilder()
                .withApplicationName("testapp")
                .withJobGroupInfo(jobGroupInfoTwo).build();
        autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV3(jobDescriptorTwo);
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-main-^1.0.0-v000");

        JobGroupInfo jobGroupInfoThree = JobGroupInfo.newBuilder()
                .withDetail("^1.0.0")
                .build();

        JobDescriptor<JobDescriptor.JobDescriptorExt> jobDescriptorThree = JobDescriptor.newBuilder()
                .withApplicationName("testapp")
                .withJobGroupInfo(jobGroupInfoThree).build();
        autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV3(jobDescriptorThree);
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-^1.0.0-v000");


        JobGroupInfo jobGroupInfoFour = JobGroupInfo.newBuilder().build();

        JobDescriptor<JobDescriptor.JobDescriptorExt> jobDescriptorFour = JobDescriptor.newBuilder()
                .withApplicationName("testapp")
                .withJobGroupInfo(jobGroupInfoFour).build();
        autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV3(jobDescriptorFour);
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-v000");
    }

    @Test
    public void checkNestedExceptionHandling() throws Exception {
        RuntimeException exceptionContainingUnknownPolicy =
                new RuntimeException
                        (new RuntimeException(
                                new RuntimeException(
                                        new RuntimeException(AutoScalePolicyException.unknownScalingPolicy("policyId", "Not found")))));
        Optional<AutoScalePolicyException> autoScalePolicyException = DefaultAppScaleManager.extractAutoScalePolicyException(exceptionContainingUnknownPolicy);

        AutoScalingPolicyTests.waitForCondition(() -> autoScalePolicyException.isPresent());
        Assertions.assertThat(autoScalePolicyException.isPresent()).isTrue();
        AutoScalingPolicyTests.waitForCondition(() -> autoScalePolicyException.get().getErrorCode() == AutoScalePolicyException.ErrorCode.UnknownScalingPolicy);
        Assertions.assertThat(autoScalePolicyException.get().getErrorCode()).isEqualTo(AutoScalePolicyException.ErrorCode.UnknownScalingPolicy);

        RuntimeException runtimeException = new RuntimeException(new RuntimeException(new Exception("Bad input")));
        Optional<AutoScalePolicyException> notAutoScalePolicyException = DefaultAppScaleManager.extractAutoScalePolicyException(runtimeException);
        AutoScalingPolicyTests.waitForCondition(() -> !notAutoScalePolicyException.isPresent());
        Assertions.assertThat(notAutoScalePolicyException.isPresent()).isFalse();
    }

    @Test
    public void checkBulkPolicyCreations() throws InterruptedException {
        AutoScalingPolicyTests.MockAlarmClient mockAlarmClient = new AutoScalingPolicyTests.MockAlarmClient();
        AutoScalingPolicyTests.MockAppAutoScalingClient mockAppAutoScalingClient = new AutoScalingPolicyTests.MockAppAutoScalingClient();
        InMemoryPolicyStore policyStore = new InMemoryPolicyStore();

        final int totalJobs = 300;
        List<String> jobIds = new ArrayList<>(totalJobs);
        for (int i = 0; i < totalJobs; i++) {
            String jobId = UUID.randomUUID().toString();
            jobIds.add(jobId);
        }

        V3JobOperations v3JobOperations = mockV3OperationsForJobs(jobIds);
        DefaultAppScaleManager appScaleManager = new DefaultAppScaleManager(policyStore, mockAlarmClient, mockAppAutoScalingClient,
                null, v3JobOperations, null, new DefaultRegistry(),
                AutoScalingPolicyTests.mockAppScaleManagerConfiguration(), Schedulers.computation());

        final CountDownLatch latch = new CountDownLatch(totalJobs);
        for (int i = 0; i < totalJobs; i++) {
            final AutoScalingPolicy autoScalingPolicy = AutoScalingPolicyTests.buildStepScalingPolicy(jobIds.get(i));
            appScaleManager.createAutoScalingPolicy(autoScalingPolicy).subscribe(pid -> {
                log.info("Policy created {}", pid);
                latch.countDown();
            });
        }

        latch.await(10, TimeUnit.SECONDS);
        Thread.sleep(2000);
        Assertions.assertThat(mockAppAutoScalingClient.getNumPolicies()).isEqualTo(totalJobs);
    }

    public static class AppScaleClientWithScalingPolicyConstraints extends AutoScalingPolicyTests.MockAppAutoScalingClient {

        Map<String, JobScalingConstraints> scalingPolicyConstraints;

        AppScaleClientWithScalingPolicyConstraints() {
            scalingPolicyConstraints = new ConcurrentHashMap<>();
        }

        @Override
        public Completable createScalableTarget(String jobId, int minCapacity, int maxCapacity) {
            JobScalingConstraints jobScalingConstraints = new JobScalingConstraints(minCapacity, maxCapacity);
            scalingPolicyConstraints.put(jobId, new JobScalingConstraints(minCapacity, maxCapacity));
            return super.createScalableTarget(jobId, jobScalingConstraints.getMinCapacity(), jobScalingConstraints.getMaxCapacity());
        }


        @Override
        public Observable<AutoScalableTarget> getScalableTargetsForJob(String jobId) {
            if (scalingPolicyConstraints.containsKey(jobId)) {
                JobScalingConstraints jobScalingConstraints = scalingPolicyConstraints.get(jobId);
                AutoScalableTarget autoScalableTarget = AutoScalableTarget.newBuilder()
                        .withMinCapacity(jobScalingConstraints.getMinCapacity())
                        .withMaxCapacity(jobScalingConstraints.getMaxCapacity())
                        .withResourceId(jobId)
                        .build();
                return Observable.just(autoScalableTarget);
            }
            return super.getScalableTargetsForJob(jobId);
        }

        JobScalingConstraints getJobScalingPolicyConstraintsForJob(String jobId) {
            return scalingPolicyConstraints.get(jobId);
        }

    }

    private List<String> submitTwoJobs(DefaultAppScaleManager appScaleManager, String jobIdOne, String jobIdTwo,
                                       InMemoryPolicyStore policyStore) throws Exception {
        // call - createAutoScalingPolicy
        AutoScalingPolicy autoScalingPolicyOne = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdOne);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyOne).toBlocking().single();

        // call - createAutoScalingPolicy
        AutoScalingPolicy autoScalingPolicyTwo = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdTwo);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyTwo).toBlocking().single();

        // call - processPendingPolicies
        List<AutoScalingPolicy> savedPolicies = policyStore.retrievePolicies(false).toList().toBlocking().first();
        AutoScalingPolicyTests.waitForCondition(() -> savedPolicies.size() == 2);
        Assertions.assertThat(savedPolicies.size()).isEqualTo(2);

        return savedPolicies.stream().map(policy -> policy.getRefId()).collect(Collectors.toList());
    }

    private JobGroupInfo buildMockJobGroupInfo(String jobId) {
        JobGroupInfo jobGroupInfo = mock(JobGroupInfo.class);
        when(jobGroupInfo.getDetail()).thenReturn("ii" + jobId);
        when(jobGroupInfo.getStack()).thenReturn("test");
        when(jobGroupInfo.getSequence()).thenReturn("001");
        return jobGroupInfo;
    }

    private V3JobOperations mockV3OperationsForJobs(List<String> jobIds) {
        V3JobOperations v3JobOperations = mock(V3JobOperations.class);

        for (String jobId : jobIds) {
            // FIXME Use JobGenerator instead of mocking.
            Job job = mock(Job.class);
            when(job.getId()).thenReturn(jobId);
            JobDescriptor jobDescriptorOne = mock(JobDescriptor.class);
            ServiceJobExt serviceJobExtOne = mock(ServiceJobExt.class);
            JobGroupInfo jobGroupInfoOne = buildMockJobGroupInfo(jobId);

            Capacity capacityOne = mock(Capacity.class);
            when(capacityOne.getMax()).thenReturn(10);
            when(capacityOne.getMin()).thenReturn(1);
            when(serviceJobExtOne.getCapacity()).thenReturn(capacityOne);
            when(jobDescriptorOne.getExtensions()).thenReturn(serviceJobExtOne);
            when(job.getJobDescriptor()).thenReturn(jobDescriptorOne);
            when(jobDescriptorOne.getJobGroupInfo()).thenReturn(jobGroupInfoOne);

            when(v3JobOperations.getJob(jobId)).thenReturn(Optional.of(job));

            JobManagerEvent<?> jobUpdateEvent = JobUpdateEvent.newJob(job);
            when(v3JobOperations.observeJobs()).thenAnswer(invocation -> Observable.from(asList(jobUpdateEvent)));
        }

        return v3JobOperations;
    }


    private V3JobOperations mockV3Operations(String jobIdOne, String jobIdTwo) {
        V3JobOperations v3JobOperations = mock(V3JobOperations.class);

        // FIXME Use JobGenerator instead of mocking.
        Job jobOne = mock(Job.class);
        when(jobOne.getId()).thenReturn(jobIdOne);
        JobDescriptor jobDescriptorOne = mock(JobDescriptor.class);
        ServiceJobExt serviceJobExtOne = mock(ServiceJobExt.class);
        JobGroupInfo jobGroupInfoOne = buildMockJobGroupInfo(jobIdOne);

        Capacity capacityOne = mock(Capacity.class);
        when(capacityOne.getMax()).thenReturn(10);
        when(capacityOne.getMin()).thenReturn(1);
        when(serviceJobExtOne.getCapacity()).thenReturn(capacityOne);
        when(jobDescriptorOne.getExtensions()).thenReturn(serviceJobExtOne);
        when(jobOne.getJobDescriptor()).thenReturn(jobDescriptorOne);
        when(jobDescriptorOne.getJobGroupInfo()).thenReturn(jobGroupInfoOne);


        Job jobTwo = mock(Job.class);
        when(jobTwo.getId()).thenReturn(jobIdTwo);
        JobDescriptor jobDescriptorTwo = mock(JobDescriptor.class);
        ServiceJobExt serviceJobExtTwo = mock(ServiceJobExt.class);
        Capacity capacityJobTwo = mock(Capacity.class);
        when(capacityJobTwo.getMin())
                .thenAnswer(new Answer<Integer>() {
                    private int count = 0;

                    @Override
                    public Integer answer(InvocationOnMock invocation) throws Throwable {
                        if (count++ < 2) {
                            return 1;
                        } else {
                            return 5;
                        }
                    }
                });

        when(capacityJobTwo.getMax()).thenAnswer(new Answer<Integer>() {
            private int count = 0;

            @Override
            public Integer answer(InvocationOnMock invocation) throws Throwable {
                if (count++ < 2) {
                    return 10;
                } else {
                    return 15;
                }
            }
        });

        when(serviceJobExtTwo.getCapacity()).thenReturn(capacityJobTwo);
        when(jobDescriptorTwo.getExtensions()).thenReturn(serviceJobExtTwo);
        when(jobDescriptorTwo.getJobGroupInfo()).thenReturn(jobGroupInfoOne);
        when(jobTwo.getJobDescriptor()).thenReturn(jobDescriptorTwo);
        when(jobTwo.getStatus()).thenReturn(JobModel.newJobStatus().withState(JobState.Accepted).build());

        when(v3JobOperations.getJob(jobIdOne)).thenReturn(Optional.of(jobOne));
        when(v3JobOperations.getJob(jobIdTwo)).thenReturn(Optional.of(jobTwo));

        JobManagerEvent<?> jobUpdateEvent = JobUpdateEvent.newJob(jobTwo);
        when(v3JobOperations.observeJobs()).thenAnswer(invocation -> Observable.from(asList(jobUpdateEvent)));

        return v3JobOperations;
    }
}
