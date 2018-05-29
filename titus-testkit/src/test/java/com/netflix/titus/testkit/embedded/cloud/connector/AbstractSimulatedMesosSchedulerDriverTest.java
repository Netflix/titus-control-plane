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

package com.netflix.titus.testkit.embedded.cloud.connector;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import com.netflix.titus.testkit.embedded.cloud.agent.OfferChangeEvent;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import com.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import com.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.copyAndRemove;
import static com.netflix.titus.common.util.CollectionsExt.first;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractSimulatedMesosSchedulerDriverTest {

    private static final String FLEX_ID = "flex1";

    private static final long TIMEOUT_MS = 5_000;

    private final SimulatedCloud cloud = SimulatedClouds.basicCloud(1);

    private final SimulatedTitusAgentCluster FLEX_INSTANCE_GROUP = cloud.getAgentInstanceGroup(FLEX_ID);
    private final String FLEX_INSTANCE_ID = FLEX_INSTANCE_GROUP.getAgents().stream().map(SimulatedTitusAgent::getId).findFirst().get();

    private final TestableScheduler callbackHandler = new TestableScheduler();

    private SchedulerDriver mesosDriver;

    private final ExtTestSubscriber<TaskExecutorHolder> launchesSubscriber = new ExtTestSubscriber<>();

    protected abstract SchedulerDriver setup(SimulatedCloud cloud, Protos.FrameworkInfo framework, Scheduler callbackHandler);

    @Before
    public void setUp() {
        mesosDriver = setup(cloud, Protos.FrameworkInfo.getDefaultInstance(), callbackHandler);
        cloud.taskLaunches().subscribe(launchesSubscriber);
        mesosDriver.run();
    }

    @Test
    public void testTaskLaunch() throws Exception {
        Protos.Offer offer = offers().takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS).get(0).getOffer();
        mesosDriver.launchTasks(singletonList(offer.getId()), singletonList(newTaskInfo()));

        // Check that task is launched
        TaskExecutorHolder holder = launchesSubscriber.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(holder).isNotNull();

        // Check that new offer is sent
        Protos.Offer newOffer = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS).getOffer();
        assertThat(newOffer.getSlaveId()).isEqualTo(offer.getSlaveId());
    }

    @Test
    public void testTaskKill() throws Exception {
        Protos.TaskInfo taskInfo = newTaskInfo();

        Protos.Offer offer = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS).getOffer();
        mesosDriver.launchTasks(singletonList(offer.getId()), singletonList(taskInfo));

        Protos.TaskState afterLaunch = taskStatusUpdates().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS).getState();
        assertThat(afterLaunch).isEqualTo(Protos.TaskState.TASK_STAGING);

        offers().skipAvailable();
        mesosDriver.killTask(taskInfo.getTaskId());

        Protos.TaskStatus afterKill = taskStatusUpdates().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(afterKill.getState()).isEqualTo(Protos.TaskState.TASK_KILLED);

        // Killing a task should result in a new offer
        Protos.Offer newOffer = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS).getOffer();
        assertThat(newOffer.getSlaveId()).isEqualTo(offer.getSlaveId());
    }

    @Test
    public void testMissingTaskKill() throws Exception {
        offers().takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        mesosDriver.killTask(Protos.TaskID.newBuilder().setValue("absent").build());

        Protos.TaskStatus taskStatus = taskStatusUpdates().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(taskStatus).isNotNull();
        assertThat(taskStatus.getState()).isEqualTo(Protos.TaskState.TASK_LOST);

        // Killing a missing task should not result in a new offer update
        assertThat(offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS)).isNull();
    }

    @Test
    public void testSpecificTaskReconcile() throws Exception {
        TaskExecutorHolder holder = launchTask();

        Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue(holder.getTaskId()))
                .setState(Protos.TaskState.TASK_RUNNING)
                .setSlaveId(Protos.SlaveID.newBuilder().setValue(holder.getAgent().getId()).build())
                .build();

        taskStatusUpdates().skipAvailable();
        mesosDriver.reconcileTasks(Collections.singleton(taskStatus));

        Protos.TaskStatus status = taskStatusUpdates().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(status.getMessage()).containsIgnoringCase("reconciler");
    }

    @Test
    public void testAllTasksReconcile() throws Exception {
        launchTask();

        mesosDriver.reconcileTasks(Collections.emptyList());

        Protos.TaskStatus status = taskStatusUpdates().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(status.getMessage()).containsIgnoringCase("reconciler");
    }

    @Test
    public void testOfferDecline() throws Exception {
        Protos.Offer offer = offers().takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS).get(0).getOffer();

        mesosDriver.declineOffer(offer.getId());
        assertThat(offers().takeNext(5, TimeUnit.SECONDS).getOffer().getSlaveId()).isEqualTo(offer.getSlaveId());
    }

    @Test
    public void testFailover() throws Exception {
        launchTask();

        mesosDriver.stop();

        TestableScheduler newCallbackHandler = new TestableScheduler();
        SchedulerDriver newDriver = setup(cloud, Protos.FrameworkInfo.getDefaultInstance(), newCallbackHandler);
        newDriver.run();

        assertThat(newCallbackHandler.offersSubscriber.takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS)).hasSize(3);
    }

    @Test
    public void testReceivesNewOffersAfterAgentGroupIsAdded() throws Exception {
        offers().takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        cloud.createAgentInstanceGroups(SimulatedAgentGroupDescriptor.awsInstanceGroup("flex2", AwsInstanceType.M4_4XLarge, 1));

        String newInstanceId = cloud.getAgentInstancesByInstanceGroup("flex2").stream().map(SimulatedTitusAgent::getId).findFirst().get();

        OfferChangeEvent offerEvent = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(offerEvent.getOffer().getSlaveId().getValue()).isEqualTo(newInstanceId);
        assertThat(offerEvent.isRescind()).isFalse();
    }

    @Test
    public void testOffersAreRescindedAfterAgentGroupIsRemoved() throws Exception {
        offers().takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        cloud.removeInstanceGroup(FLEX_ID);

        OfferChangeEvent offerEvent = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(offerEvent.getOffer().getSlaveId().getValue()).isEqualTo(FLEX_INSTANCE_ID);
        assertThat(offerEvent.isRescind()).isTrue();
    }

    @Test
    public void testReceivesNewOffersAfterAgentInstanceIsAdded() throws Exception {
        offers().takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        cloud.updateAgentGroupCapacity(FLEX_ID, 0, 2, 3);

        Set<String> instanceIds = cloud.getAgentInstancesByInstanceGroup(FLEX_ID).stream().map(SimulatedTitusAgent::getId).collect(Collectors.toSet());
        String newInstanceId = first(copyAndRemove(instanceIds, FLEX_INSTANCE_ID));

        OfferChangeEvent offerEvent = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(offerEvent.getOffer().getSlaveId().getValue()).isEqualTo(newInstanceId);
        assertThat(offerEvent.isRescind()).isFalse();
    }

    @Test
    public void testOffersAreRescindedAfterAgentInstanceIsRemoved() throws Exception {
        offers().takeNext(3, TIMEOUT_MS, TimeUnit.MILLISECONDS);
        cloud.updateAgentGroupCapacity(FLEX_ID, 0, 0, 3);

        assertThat(cloud.getAgentInstancesByInstanceGroup(FLEX_ID)).isEmpty();

        OfferChangeEvent offerEvent = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(offerEvent.getOffer().getSlaveId().getValue()).isEqualTo(FLEX_INSTANCE_ID);
        assertThat(offerEvent.isRescind()).isTrue();
    }

    private ExtTestSubscriber<OfferChangeEvent> offers() {
        return callbackHandler.offersSubscriber;
    }

    private ExtTestSubscriber<Protos.TaskStatus> taskStatusUpdates() {
        return callbackHandler.taskStatusUpdatesSubscriber;
    }

    private Protos.TaskInfo newTaskInfo() {
        return Protos.TaskInfo.newBuilder()
                .setTaskId(Protos.TaskID.newBuilder().setValue("myTask"))
                .setName("myTask")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave").build())
                .setData(TitanProtos.ContainerInfo.newBuilder()
                        .putTitusProvidedEnv("TITUS_JOB_ID", "myJob")
                        .setNetworkConfigInfo(TitanProtos.ContainerInfo.NetworkConfigInfo.newBuilder()
                                .setEniLabel("eni0")
                                .setEniLablel("eni0")
                                .addSecurityGroups("sg-123456")
                        )
                        .putTitusProvidedEnv("TITUS_JOB_ID", "testJob")
                        .build()
                        .toByteString()
                )
                .build();
    }

    private TaskExecutorHolder launchTask() throws Exception {
        Protos.Offer offer = offers().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS).getOffer();
        offers().skipAvailable();

        mesosDriver.launchTasks(singletonList(offer.getId()), singletonList(newTaskInfo()));
        TaskExecutorHolder holder = launchesSubscriber.takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        Protos.TaskStatus afterLaunch = taskStatusUpdates().takeNext(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertThat(afterLaunch.getState()).isEqualTo(Protos.TaskState.TASK_STAGING);

        return holder;
    }

    private class TestableScheduler implements Scheduler {

        private final ExtTestSubscriber<OfferChangeEvent> offersSubscriber = new ExtTestSubscriber<>();
        private final ExtTestSubscriber<Protos.TaskStatus> taskStatusUpdatesSubscriber = new ExtTestSubscriber<>();

        private final Map<Protos.OfferID, Protos.Offer> offerMap = new HashMap<>();

        @Override
        public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {

        }

        @Override
        public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {

        }

        @Override
        public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
            offers.forEach(offer -> {
                offerMap.put(offer.getId(), offer);
                offersSubscriber.onNext(OfferChangeEvent.offer(offer));
            });
        }

        @Override
        public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
            Protos.Offer offer = offerMap.remove(offerId);
            if (offer != null) {
                offersSubscriber.onNext(OfferChangeEvent.rescind(offer));
            }
        }

        @Override
        public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
            taskStatusUpdatesSubscriber.onNext(status);
        }

        @Override
        public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {

        }

        @Override
        public void disconnected(SchedulerDriver driver) {

        }

        @Override
        public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {

        }

        @Override
        public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {

        }

        @Override
        public void error(SchedulerDriver driver, String message) {

        }
    }
}
