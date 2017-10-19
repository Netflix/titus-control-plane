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

package io.netflix.titus.master.appscale.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.appscale.model.AutoScalableTarget;
import io.netflix.titus.api.appscale.model.AutoScalingPolicy;
import io.netflix.titus.api.appscale.model.PolicyType;
import io.netflix.titus.api.jobmanager.model.event.JobUpdateEvent;
import io.netflix.titus.api.jobmanager.model.job.Capacity;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.JobGroupInfo;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.V3JobOperations;
import io.netflix.titus.api.model.event.JobStateChangeEvent;
import io.netflix.titus.api.model.v2.V2JobDefinition;
import io.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import io.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.common.framework.reconciler.ReconcilerEvent;
import io.netflix.titus.common.util.rx.eventbus.RxEventBus;
import io.netflix.titus.common.util.rx.eventbus.internal.DefaultRxEventBus;
import io.netflix.titus.master.job.V2JobMgrIntf;
import io.netflix.titus.master.job.V2JobOperations;
import io.netflix.titus.master.jobmanager.SampleTitusModelUpdateActions;
import io.netflix.titus.runtime.store.v3.memory.InMemoryPolicyStore;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

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
    public void checkStepCreatePolicyFlow() {
        checkCreatePolicyFlow(PolicyType.StepScaling);
    }

    public void checkCreatePolicyFlow(PolicyType policyType) {
        // create instance of DefaultAppScaleManager
        AutoScalingPolicyTests.MockAlarmClient mockAlarmClient = new AutoScalingPolicyTests.MockAlarmClient();
        AutoScalingPolicyTests.MockAppAutoScalingClient mockAppAutoScalingClient = new AutoScalingPolicyTests.MockAppAutoScalingClient();
        InMemoryPolicyStore policyStore = new InMemoryPolicyStore();
        String jobIdOne = UUID.randomUUID().toString();
        String jobIdTwo = UUID.randomUUID().toString();
        V3JobOperations v3JobOperations = mockV3Operations(jobIdOne, jobIdTwo);

        DefaultAppScaleManager appScaleManager = new DefaultAppScaleManager(policyStore, mockAlarmClient, mockAppAutoScalingClient,
                null, v3JobOperations, null, new DefaultRegistry());

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
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyTwo).toBlocking().single();

        // call - processPendingPolicies
        List<String> refIdsCreated = appScaleManager.processPendingPolicyRequests().toList().toBlocking().first();
        Assertions.assertThat(refIdsCreated.size()).isEqualTo(2);

        // verify counts in CloudAlarmClient, AppAutoScaleClient and AppScalePolicyStore
        List<AutoScalingPolicy> policiesStored = policyStore.retrievePolicies().toList().toBlocking().first();
        Assertions.assertThat(policiesStored.size()).isEqualTo(2);
        Assertions.assertThat(mockAppAutoScalingClient.getNumPolicies()).isEqualTo(2);
        Assertions.assertThat(mockAppAutoScalingClient.getNumScalableTargets()).isEqualTo(2);
        if (policyType == PolicyType.StepScaling) {
            Assertions.assertThat(mockAlarmClient.getNumOfAlarmsCreated()).isEqualTo(2);
        }

        String policyRefIdToDelete = refIdsCreated.get(0);
        appScaleManager.removeAutoScalingPolicy(policyRefIdToDelete).await();
        List<String> refIdsDeleted = appScaleManager.processDeletingPolicyRequests().toList().toBlocking().first();
        Assertions.assertThat(refIdsDeleted.size()).isEqualTo(1);

        // verify counts in CloudAlarmClient, AppAutoScaleClient and AppScalePolicyStore
        policiesStored = policyStore.retrievePolicies().toList().toBlocking().first();
        Assertions.assertThat(policiesStored.size()).isEqualTo(1);
        Assertions.assertThat(mockAppAutoScalingClient.getNumPolicies()).isEqualTo(1);
        Assertions.assertThat(mockAppAutoScalingClient.getNumScalableTargets()).isEqualTo(1);
        if (policyType == PolicyType.StepScaling) {
            Assertions.assertThat(mockAlarmClient.getNumOfAlarmsCreated()).isEqualTo(1);
        }
    }

    @Test
    public void checkV2LiveStreamPolicyCleanup() throws InterruptedException {
        AutoScalingPolicyTests.MockAlarmClient mockAlarmClient = new AutoScalingPolicyTests.MockAlarmClient();
        AutoScalingPolicyTests.MockAppAutoScalingClient mockAppAutoScalingClient = new AutoScalingPolicyTests.MockAppAutoScalingClient();
        InMemoryPolicyStore policyStore = new InMemoryPolicyStore();
        Registry registry = new DefaultRegistry();
        RxEventBus eventBus = new DefaultRxEventBus(registry.createId("test"), registry);


        DefaultAppScaleManager appScaleManager = new DefaultAppScaleManager(policyStore, mockAlarmClient, mockAppAutoScalingClient,
                mockV2Operations(), null, eventBus, registry);

        // call - createAutoScalingPolicy
        String jobIdOne = "Titus-1";
        AutoScalingPolicy autoScalingPolicyOne = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdOne);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyOne).toBlocking().single();

        // call - createAutoScalingPolicy
        String jobIdTwo = "Titus-2";
        AutoScalingPolicy autoScalingPolicyTwo = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdTwo);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyTwo).toBlocking().single();

        // call - processPendingPolicies
        List<String> refIdsCreated = appScaleManager.processPendingPolicyRequests().toList().toBlocking().first();
        Assertions.assertThat(refIdsCreated.size()).isEqualTo(2);

        log.info("Done creating two policies");

        CountDownLatch latch = new CountDownLatch(1);
        Observable<AutoScalingPolicy> autoScalingPolicyObservable = appScaleManager.v2LiveStreamPolicyCleanup();
        List<AutoScalingPolicy> policiesToBeCleaned = new ArrayList<>();
        autoScalingPolicyObservable.subscribe(autoScalingPolicy -> {
                    log.info("Got AutoScalingPolicy to be cleaned up {}", autoScalingPolicy);
                    policiesToBeCleaned.add(autoScalingPolicy);
                    latch.countDown();
                },
                e -> log.error("Error in v2 live stream for policy cleanup"),
                () -> log.info("Completed"));

        eventBus.publish(new JobStateChangeEvent<>(jobIdTwo, JobStateChangeEvent.JobState.Finished,
                System.currentTimeMillis(), "jobFinished"));
        log.info("Done publishing JobStateChangeEvent for {}", jobIdTwo);

        latch.await(3, TimeUnit.SECONDS);
        Assertions.assertThat(policiesToBeCleaned.size()).isEqualTo(1);
        Assertions.assertThat(policiesToBeCleaned.get(0).getJobId()).isEqualTo(jobIdTwo);


        List<String> refIdsDeleted = appScaleManager.processDeletingPolicyRequests().toList().toBlocking().first();
        Assertions.assertThat(refIdsDeleted.size()).isEqualTo(1);
    }

    @Test
    public void checkV2LiveStreamTargetUpdates() throws InterruptedException {
        InMemoryPolicyStore policyStore = new InMemoryPolicyStore();
        Registry registry = new DefaultRegistry();
        RxEventBus eventBus = new DefaultRxEventBus(registry.createId("test"), registry);


        V2JobOperations v2JobOperations = mockV2Operations();
        AppScaleClientWithScalingPolicyConstraints appScalingClient = new AppScaleClientWithScalingPolicyConstraints();
        DefaultAppScaleManager appScaleManager = new DefaultAppScaleManager(policyStore,
                new AutoScalingPolicyTests.MockAlarmClient(),
                appScalingClient,
                v2JobOperations, null, eventBus, registry);

        // call - createAutoScalingPolicy
        String jobIdOne = "Titus-1";
        AutoScalingPolicy autoScalingPolicyOne = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdOne);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyOne).toBlocking().single();

        // call - createAutoScalingPolicy
        String jobIdTwo = "Titus-2";
        AutoScalingPolicy autoScalingPolicyTwo = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdTwo);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyTwo).toBlocking().single();

        // call - processPendingPolicies
        List<String> refIdsCreated = appScaleManager.processPendingPolicyRequests().toList().toBlocking().first();
        Assertions.assertThat(refIdsCreated.size()).isEqualTo(2);

        log.info("Done creating two policies");

        CountDownLatch latch = new CountDownLatch(1);
        Observable<AutoScalableTarget> autoScalableTargetObservable = appScaleManager.v2LiveStreamTargetUpdates();

        List<AutoScalableTarget> targetsUpdated = new ArrayList<>();
        autoScalableTargetObservable.subscribe(targetUpdated -> {
                    log.info("Got ScalableTarget to be updated {}", targetUpdated);
                    targetsUpdated.add(targetUpdated);
                    latch.countDown();
                },
                e -> log.error("Error in v2 live stream for scalable target update"),
                () -> log.info("Completed"));

        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdTwo).getMinCapacity()).isEqualTo(1);
        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdTwo).getMaxCapacity()).isEqualTo(10);

        eventBus.publish(new JobStateChangeEvent<>(jobIdTwo, JobStateChangeEvent.JobState.Created,
                System.currentTimeMillis(), "jobUpdated"));
        log.info("Done publishing JobStateChangeEvent for {}", jobIdTwo);

        latch.await(3, TimeUnit.SECONDS);
        Assertions.assertThat(targetsUpdated.size()).isEqualTo(1);
        Assertions.assertThat(targetsUpdated.get(0).getMinCapacity()).isEqualTo(5);
        Assertions.assertThat(targetsUpdated.get(0).getMaxCapacity()).isEqualTo(15);
    }

    @Test
    public void checkV3LiveStreamTargetUpdates() throws InterruptedException {
        String jobIdOne = UUID.randomUUID().toString();
        String jobIdTwo = UUID.randomUUID().toString();

        InMemoryPolicyStore policyStore = new InMemoryPolicyStore();
        V3JobOperations v3JobOperations = mockV3Operations(jobIdOne, jobIdTwo);
        AppScaleClientWithScalingPolicyConstraints appScalingClient = new AppScaleClientWithScalingPolicyConstraints();
        DefaultAppScaleManager appScaleManager = new DefaultAppScaleManager(policyStore,
                new AutoScalingPolicyTests.MockAlarmClient(),
                appScalingClient,
                null,
                v3JobOperations, null, new DefaultRegistry());

        List<String> refIds = submitTwoJobs(appScaleManager, jobIdOne, jobIdTwo);
        Assertions.assertThat(refIds.size()).isEqualTo(2);

        CountDownLatch latch = new CountDownLatch(1);
        Observable<AutoScalableTarget> autoScalableTargetObservable = appScaleManager.v3LiveStreamTargetUpdates();

        List<AutoScalableTarget> targetsUpdated = new ArrayList<>();
        autoScalableTargetObservable.subscribe(targetUpdated -> {
                    log.info("Got ScalableTarget to be updated {}", targetUpdated);
                    Assertions.assertThat(targetUpdated.getResourceId()).isEqualTo(jobIdTwo);
                    targetsUpdated.add(targetUpdated);
                    latch.countDown();
                },
                e -> log.error("Error in v2 live stream for scalable target update"),
                () -> log.info("Completed"));

        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdOne).getMinCapacity()).isEqualTo(1);
        Assertions.assertThat(appScalingClient.getJobScalingPolicyConstraintsForJob(jobIdOne).getMaxCapacity()).isEqualTo(10);

        latch.await(3, TimeUnit.SECONDS);
        Assertions.assertThat(targetsUpdated.size()).isEqualTo(1);
        Assertions.assertThat(targetsUpdated.get(0).getMinCapacity()).isEqualTo(5);
        Assertions.assertThat(targetsUpdated.get(0).getMaxCapacity()).isEqualTo(15);
    }


    private V3JobOperations mockV3Operations(String jobIdOne, String jobIdTwo) {
        V3JobOperations v3JobOperations = mock(V3JobOperations.class);

        Job jobOne = mock(Job.class);
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
        JobDescriptor jobDescriptorTwo = mock(JobDescriptor.class);
        ServiceJobExt serviceJobExtTwo = mock(ServiceJobExt.class);
        Capacity capacityJobTwo = mock(Capacity.class);
        JobGroupInfo jobGroupInfoTwo = buildMockJobGroupInfo(jobIdTwo);
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

        when(v3JobOperations.getJob(jobIdOne)).thenReturn(Optional.of(jobOne));
        when(v3JobOperations.getJob(jobIdTwo)).thenReturn(Optional.of(jobTwo));


        JobUpdateEvent jobUpdateEvent =
                new JobUpdateEvent(ReconcilerEvent.EventType.Changed, SampleTitusModelUpdateActions.any(jobIdTwo),
                        Optional.of(jobTwo), Optional.of(jobTwo), Optional.empty());
        when(v3JobOperations.observeJobs())
                .thenAnswer(invocation -> Observable.from(Arrays.asList(jobUpdateEvent)));

        return v3JobOperations;
    }


    private V2JobOperations mockV2Operations() {
        V2JobMgrIntf mockJobMgr = mock(V2JobMgrIntf.class);
        V2JobOperations mockV2JobOperations = mock(V2JobOperations.class);
        V2JobDefinition mockJobDefinition = mock(V2JobDefinition.class);
        StageSchedulingInfo mockStageSchedulingInfo = mock(StageSchedulingInfo.class);
        StageScalingPolicy mockScalingPolicy = mock(StageScalingPolicy.class);
        SchedulingInfo mockSchedulingInfo = mock(SchedulingInfo.class);


        when(mockScalingPolicy.getMin())
                .thenAnswer(new Answer<Integer>() {
                    private int count = 0;

                    @Override
                    public Integer answer(InvocationOnMock invocation) throws Throwable {
                        if (count++ < 3) {
                            return 1;
                        } else {
                            return 5;
                        }
                    }
                });

        when(mockScalingPolicy.getMax()).thenAnswer(new Answer<Integer>() {
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

        when(mockStageSchedulingInfo.getScalingPolicy()).thenReturn(mockScalingPolicy);

        Map<Integer, StageSchedulingInfo> stageMap = new HashMap<>();
        stageMap.put(1, mockStageSchedulingInfo);

        when(mockSchedulingInfo.getStages()).thenReturn(stageMap);
        when(mockJobDefinition.getSchedulingInfo()).thenReturn(mockSchedulingInfo);

        when(mockJobMgr.getJobDefinition()).thenReturn(mockJobDefinition);

        when(mockV2JobOperations.getJobMgr(anyString())).thenReturn(mockJobMgr);

        return mockV2JobOperations;
    }

    @Test
    public void checkASGNameBuildingV2() {
        Parameter appParam = new Parameter(Parameters.APP_NAME, "testapp");
        Parameter stackParam = new Parameter(Parameters.JOB_GROUP_STACK, "main");
        Parameter detailParam = new Parameter(Parameters.JOB_GROUP_DETAIL, "2.0.0");
        Parameter seqParam = new Parameter(Parameters.JOB_GROUP_SEQ, "v000");

        String autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV2(Arrays.asList(appParam, stackParam, detailParam, seqParam));
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-main-2.0.0-v000");


        autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV2(Arrays.asList(stackParam, appParam, detailParam, seqParam));
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-main-2.0.0-v000");


        autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV2(Arrays.asList(stackParam, appParam));
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-main-v000");

        autoScalingGroup = DefaultAppScaleManager.buildAutoScalingGroupV2(Arrays.asList(appParam));
        Assertions.assertThat(autoScalingGroup).isEqualTo("testapp-v000");

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

    public static class AppScaleClientWithScalingPolicyConstraints extends AutoScalingPolicyTests.MockAppAutoScalingClient {

        Map<String, DefaultAppScaleManager.JobScalingConstraints> scalingPolicyConstraints;

        public AppScaleClientWithScalingPolicyConstraints() {
            scalingPolicyConstraints = new ConcurrentHashMap<>();
        }

        @Override
        public Completable createScalableTarget(String jobId, int minCapacity, int maxCapacity) {
            DefaultAppScaleManager.JobScalingConstraints jobScalingConstraints = new DefaultAppScaleManager.JobScalingConstraints(minCapacity, maxCapacity);
            scalingPolicyConstraints.put(jobId, new DefaultAppScaleManager.JobScalingConstraints(minCapacity, maxCapacity));
            return super.createScalableTarget(jobId, jobScalingConstraints.getMinCapacity(), jobScalingConstraints.getMaxCapacity());
        }


        @Override
        public Observable<AutoScalableTarget> getScalableTargetsForJob(String jobId) {
            if (scalingPolicyConstraints.containsKey(jobId)) {
                DefaultAppScaleManager.JobScalingConstraints jobScalingConstraints = scalingPolicyConstraints.get(jobId);
                AutoScalableTarget autoScalableTarget = AutoScalableTarget.newBuilder()
                        .withMinCapacity(jobScalingConstraints.getMinCapacity())
                        .withMaxCapacity(jobScalingConstraints.getMaxCapacity())
                        .withResourceId(jobId)
                        .build();
                return Observable.just(autoScalableTarget);
            }
            return super.getScalableTargetsForJob(jobId);
        }

        public DefaultAppScaleManager.JobScalingConstraints getJobScalingPolicyConstraintsForJob(String jobId) {
            return scalingPolicyConstraints.get(jobId);
        }
    }

    private List<String> submitTwoJobs(DefaultAppScaleManager appScaleManager, String jobIdOne, String jobIdTwo) {
        // call - createAutoScalingPolicy
        AutoScalingPolicy autoScalingPolicyOne = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdOne);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyOne).toBlocking().single();

        // call - createAutoScalingPolicy
        AutoScalingPolicy autoScalingPolicyTwo = AutoScalingPolicyTests.buildStepScalingPolicy(jobIdTwo);
        appScaleManager.createAutoScalingPolicy(autoScalingPolicyTwo).toBlocking().single();

        // call - processPendingPolicies
        List<String> refIdsCreated = appScaleManager.processPendingPolicyRequests().toList().toBlocking().first();
        Assertions.assertThat(refIdsCreated.size()).isEqualTo(2);
        return refIdsCreated;
    }

    private JobGroupInfo buildMockJobGroupInfo(String jobId) {
        JobGroupInfo jobGroupInfo = mock(JobGroupInfo.class);
        when(jobGroupInfo.getDetail()).thenReturn("ii" + jobId);
        when(jobGroupInfo.getStack()).thenReturn("test");
        when(jobGroupInfo.getSequence()).thenReturn("001");
        return jobGroupInfo;
    }
}
