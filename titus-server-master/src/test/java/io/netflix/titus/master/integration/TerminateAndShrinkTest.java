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

package io.netflix.titus.master.integration;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.common.network.client.RxRestClientException;
import io.netflix.titus.master.endpoint.v2.rest.representation.JobSetInstanceCountsCmd;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.embedded.cloud.SimulatedClouds;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMasters;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.JobObserver;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import io.netflix.titus.testkit.util.TitusTaskIdParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static io.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates.basicSetupActivation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Category(IntegrationTest.class)
public class TerminateAndShrinkTest extends BaseIntegrationTest {

    static {
        Logger.getLogger("io.netflix.titus.master.job.JobMgrUtils").setLevel(Level.DEBUG);
    }

    private final TitusMasterResource titusMasterResource = new TitusMasterResource(EmbeddedTitusMasters.basicMaster(SimulatedClouds.basicCloud(10)));

    private InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusMasterResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusMasterResource).around(instanceGroupsScenarioBuilder);

    private EmbeddedTitusMaster titusMaster;

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator();

    private TitusJobSpec oneTaskServiceSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
            .instancesMin(0).instancesDesired(1).instancesMax(3)
            .build();

    private TitusJobSpec twoTaskServiceSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
            .instancesMin(0).instancesDesired(2).instancesMax(3)
            .build();

    private TitusJobSpec threeTaskServiceSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
            .instancesMin(0).instancesDesired(3).instancesMax(5)
            .build();

    private TitusMasterClient client;
    private JobRunner jobRunner;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(basicSetupActivation());

        titusMaster = titusMasterResource.getMaster();

        client = titusMaster.getClient();
        jobRunner = new JobRunner(titusMaster);
    }

    @Test(timeout = 30000)
    public void testTerminateAndShrinkScalesDownJobByOne() throws Exception {
        List<TaskExecutorHolder> holders = jobRunner.runJob(twoTaskServiceSpec);
        TaskExecutorHolder firstHolder = holders.get(0);
        String jobId = firstHolder.getJobId();

        // Terminate and shrink first task
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.terminateAndShrink(firstHolder.getTaskId());

        // Check the desired size is set to 1
        TitusJobInfo jobInfo = client.findJob(jobId, false).toBlocking().first();
        assertThat(jobInfo.getInstancesDesired()).isEqualTo(1);
    }

    @Test
    public void testTerminateAndShrinkForTerminatedTask() throws Exception {
        List<TaskExecutorHolder> holders = jobRunner.runJob(twoTaskServiceSpec);
        TaskExecutorHolder firstHolder = holders.get(0);
        String jobId = firstHolder.getJobId();

        // Terminate and shrink task twice
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.terminateAndShrink(firstHolder.getTaskId());
        jobObserver.terminateAndShrink(firstHolder.getTaskId());

        // Check the desired size is set to 1
        TitusJobInfo jobInfo = client.findJob(jobId, false).toBlocking().first();
        assertThat(jobInfo.getInstancesDesired()).isEqualTo(1);
    }

    @Test(timeout = 30000)
    public void testTerminateAndShrinkRecyclesTombStonedIndexes() throws Exception {
        List<TaskExecutorHolder> holders = jobRunner.runJob(twoTaskServiceSpec);
        TaskExecutorHolder firstHolder = holders.get(0);
        TaskExecutorHolder secondHolder = holders.get(1);
        String jobId = firstHolder.getJobId();

        // Terminate and shrink first task
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.terminateAndShrink(firstHolder.getTaskId());

        // Scale up by one
        autoStartNewTasks();
        jobObserver.updateJobSize(2, 1, 3);

        // Verify the new task id has the same task index as the one terminated
        TitusJobInfo scaledJob = client.findJob(jobId, false).toBlocking().first();
        Set<String> newTaskIds = scaledJob.getTasks().stream().map(TaskInfo::getId).collect(Collectors.toSet());
        assertThat(newTaskIds).contains(secondHolder.getTaskId());

        newTaskIds.remove(secondHolder.getTaskId());
        verifyTasksHaveTheSameIndex(newTaskIds.iterator().next(), firstHolder.getTaskId());
    }

    @Test(timeout = 30000)
    public void testScaleUpWithTombStonedIndexes() throws Exception {
        List<TaskExecutorHolder> holders = jobRunner.runJob(twoTaskServiceSpec);
        TaskExecutorHolder firstHolder = holders.get(0);
        String jobId = firstHolder.getJobId();

        // Terminate and shrink first task
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.terminateAndShrink(firstHolder.getTaskId());

        // Now resize the job
        autoStartNewTasks();
        jobObserver.updateJobSize(3, 1, 5); // Fails if job not scaled to desired level = 3
    }

    @Test(timeout = 30000)
    public void testScaleDownWithTombStonedIndexes() throws Exception {
        List<TaskExecutorHolder> holders = jobRunner.runJob(threeTaskServiceSpec);
        TaskExecutorHolder secondHolder = holders.get(1);
        String jobId = secondHolder.getJobId();

        // Terminate and shrink first task
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.terminateAndShrink(secondHolder.getTaskId());

        // Now resize the job
        autoStartNewTasks();
        jobObserver.updateJobSize(1, 1, 5); // Scale down from 2 to 1
    }

    @Test(timeout = 30000)
    public void testTombStonedTaskIsNotRunningAfterMasterReboot() throws Exception {
        List<TaskExecutorHolder> holders = jobRunner.runJob(twoTaskServiceSpec);
        TaskExecutorHolder firstHolder = holders.get(0);
        String jobId = firstHolder.getJobId();

        // Terminate and shrink first task
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.terminateAndShrink(firstHolder.getTaskId());

        titusMaster.reboot();

        // Check the desired size is set to 1, and only one task is running
        int running = jobObserver.awaitJobInState(TitusTaskState.RUNNING);
        assertThat(running).isEqualTo(1);

        // Now resize the job
        autoStartNewTasks();
        jobObserver.updateJobSize(2, 1, 3); // Fails if job not scaled to desired level = 2
    }

    @Test(timeout = 30000)
    public void testScalingToZeroDoesNotTerminateJob() throws Exception {
        List<TaskExecutorHolder> holders = jobRunner.runJob(oneTaskServiceSpec);
        TaskExecutorHolder firstHolder = holders.get(0);
        String jobId = firstHolder.getJobId();

        // Terminate and shrink first task
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        jobObserver.terminateAndShrink(firstHolder.getTaskId());

        TitusJobInfo zeroTaskJob = client.findJob(jobId, false).toBlocking().first();
        Assertions.assertThat(zeroTaskJob.getTasks()).isEmpty();

        // Scale up again
        autoStartNewTasks();
        jobObserver.updateJobSize(1, 0, 3); // Fails if job not scaled to desired level = 1
    }

    @Test(timeout = 30000)
    public void testScalingBelowMinNotAllowed() throws Exception {
        TitusJobSpec myTask = new TitusJobSpec.Builder(oneTaskServiceSpec).instancesMin(1).build();
        List<TaskExecutorHolder> holders = jobRunner.runJob(myTask);
        TaskExecutorHolder firstHolder = holders.get(0);
        String jobId = firstHolder.getJobId();

        // Terminate and shrink first task
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);
        try {
            jobObserver.terminateAndShrink(firstHolder.getTaskId());
            fail("Expected terminate and shrink operation to fail");
        } catch (RxRestClientException e) {
            assertThat(e.getStatusCode()).isEqualTo(400);
        }
    }

    @Test
    public void testScalingUpWithMultipleTombstones() throws Exception {
        TitusJobSpec largeTaskSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "myjob"))
                .instancesMin(0).instancesDesired(1).instancesMax(100)
                .build();

        List<TaskExecutorHolder> holders = jobRunner.runJob(largeTaskSpec);
        String jobId = holders.get(0).getJobId();
        JobObserver jobObserver = new JobObserver(jobId, titusMaster);

        // Scale up
        List<TaskExecutorHolder> scaleUp1Holders = new ArrayList<>();
        titusMaster.observeLaunchedTasks().subscribe(scaleUp1Holders::add);

        autoStartNewTasks();
        jobObserver.updateJobSize(30, 0, 100);

        // Terminate & shrink
        for (int i = 0; i < 30; i += 2) {
            jobObserver.terminateAndShrink(scaleUp1Holders.get(i).getTaskId());
        }

        // Scale down to 1
        jobObserver.updateJobSize(1, 0, 30);

        // Now scale up
        List<TaskExecutorHolder> newHolders = new ArrayList<>();
        titusMaster.observeLaunchedTasks().subscribe(newHolders::add);

        client.setInstanceCount(
                new JobSetInstanceCountsCmd("myUser", jobId, 11, 0, 100)
        ).toBlocking().firstOrDefault(null);
        client.setInstanceCount(
                new JobSetInstanceCountsCmd("myUser", jobId, 21, 0, 100)
        ).toBlocking().firstOrDefault(null);

        Thread.sleep(1000);
        assertThat(newHolders).hasSize(20);
        List<TaskExecutorHolder> invalidIndexes = newHolders.stream().filter(h -> TitusTaskIdParser.getTaskIndexFromTaskId(h.getTaskId()) > 20).collect(Collectors.toList());
        assertThat(invalidIndexes).isEmpty();
    }

    private void verifyTasksHaveTheSameIndex(String first, String second) {
        int firstIndex = TitusTaskIdParser.getTaskIndexFromTaskId(first);
        int secondIndex = TitusTaskIdParser.getTaskIndexFromTaskId(second);
        assertThat(firstIndex).isEqualTo(secondIndex);
    }

    private void autoStartNewTasks() {
        titusMaster.observeLaunchedTasks().subscribe(
                holder -> {
                    holder.transitionTo(Protos.TaskState.TASK_STARTING);
                    holder.transitionTo(Protos.TaskState.TASK_RUNNING);
                }
        );
    }
}
