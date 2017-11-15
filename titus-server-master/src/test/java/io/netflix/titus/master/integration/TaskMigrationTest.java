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
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceBlockingStub;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import org.assertj.core.api.Condition;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A collection of integration tests that test task migration.
 */
@Category(IntegrationTest.class)
public class TaskMigrationTest extends BaseIntegrationTest {

    private SimulatedCloud simulatedCloud = new SimulatedCloud();

    private SimulatedTitusAgentCluster agentClusterOne = aTitusAgentCluster("agentClusterOne", 0)
            .withComputeResources(simulatedCloud.getComputeResources())
            .withSize(3)
            .withInstanceType(AwsInstanceType.M3_XLARGE)
            .build();
    private SimulatedTitusAgentCluster agentClusterTwo = aTitusAgentCluster("agentClusterTwo", 1)
            .withComputeResources(simulatedCloud.getComputeResources())
            .withSize(3)
            .withInstanceType(AwsInstanceType.M3_XLARGE)
            .build();

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withSimulatedCloud(simulatedCloud)
                    .withProperty("titus.scheduler.tierSlaUpdateIntervalMs", "10")
                    .withProperty("titus.master.capacityManagement.availableCapacityUpdateIntervalMs", "10")
                    .withCriticalTier(0.1, AwsInstanceType.M4_4XLarge)
                    .withFlexTier(0.1, AwsInstanceType.M3_XLARGE)
                    .withAgentCluster(agentClusterOne)
                    .withAgentCluster(agentClusterTwo)
                    .withProperty("titus.master.taskMigration.schedulerDelayInMillis", "100")
                    .build()
    );

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator();

    private TitusMasterClient client;
    private AgentManagementServiceBlockingStub agentClient;

    private JobRunner jobRunner;

    @Before
    public void setUp() throws Exception {
        EmbeddedTitusMaster titusMaster = titusMasterResource.getMaster();

        client = titusMaster.getClient();
        agentClient = titusMaster.getV3BlockingGrpcAgentClient();
        jobRunner = new JobRunner(titusMaster);
    }

    @Test(timeout = 30_000)
    public void doNotMigrateBatchJob() throws Exception {
        setActiveCluster(singletonList(agentClusterOne.getName()));

        List<TaskExecutorHolder> holders = jobRunner.runJob(generator.newJobSpec(TitusJobType.batch, "batchJob"));
        TaskExecutorHolder batchTaskHolder = holders.get(0);

        TitusTaskInfo batchTask = client.findTask(batchTaskHolder.getTaskId(), false).toBlocking().first();
        assertThat(isHostOnAgentCluster(batchTask.getHost(), agentClusterOne)).isTrue();

        setActiveCluster(singletonList(agentClusterTwo.getName()));

        TaskExecutorHolder taskExecutorHolder = waitForTaskMigration(batchTask);
        assertThat(taskExecutorHolder).isNull();
    }

    @Test(timeout = 30_000)
    public void migrateServiceJob() throws Exception {
        setActiveCluster(singletonList(agentClusterOne.getName()));

        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "serviceJob"))
                .instancesMin(1).instancesDesired(1).instancesMax(1)
                .build();
        List<TaskExecutorHolder> holders = jobRunner.runJob(jobSpec);
        TaskExecutorHolder firstHolder = holders.get(0);

        TitusTaskInfo serviceTask = client.findTask(firstHolder.getTaskId(), false).toBlocking().first();
        assertThat(isHostOnAgentCluster(serviceTask.getHost(), agentClusterOne)).isTrue();

        setActiveCluster(singletonList(agentClusterTwo.getName()));

        TaskExecutorHolder taskExecutorHolder = waitForTaskMigration(serviceTask);
        TitusTaskInfo titusTaskInfo = client.findTask(taskExecutorHolder.getTaskId(), false).toBlocking().firstOrDefault(null);
        assertThat(isHostOnAgentCluster(titusTaskInfo.getHost(), agentClusterTwo)).isTrue();

        TitusJobInfo titusJobInfo = client.findJob(titusTaskInfo.getJobId()).toBlocking().firstOrDefault(null);
        assertThat(titusJobInfo.getTasks().size()).isEqualTo(jobSpec.getInstances());
    }

    @Test(timeout = 30_000)
    public void migrateServiceJobAndVerifyThatOnlyOneTaskIsMigratedFirst() throws Exception {
        setActiveCluster(singletonList(agentClusterOne.getName()));

        TitusJobSpec jobSpec = new TitusJobSpec.Builder(generator.newJobSpec(TitusJobType.service, "serviceJob"))
                .instancesMin(3).instancesDesired(3).instancesMax(3)
                .build();

        List<TaskExecutorHolder> holders = jobRunner.runJob(jobSpec);
        List<TitusTaskInfo> taskInfos = new ArrayList<>();
        for (TaskExecutorHolder holder : holders) {
            TitusTaskInfo taskInfo = client.findTask(holder.getTaskId(), false).toBlocking().first();
            assertThat(isHostOnAgentCluster(taskInfo.getHost(), agentClusterOne)).isTrue();
            taskInfos.add(taskInfo);
        }

        setActiveCluster(singletonList(agentClusterTwo.getName()));

        TitusTaskInfo first = taskInfos.get(0);

        waitForTaskMigration(first);
        TitusJobInfo titusJobInfo = client.findJob(first.getJobId()).toBlocking().firstOrDefault(null);
        List<TaskInfo> tasks = titusJobInfo.getTasks();

        Condition<TaskInfo> areRunningOnAgentClusterOne = new Condition<>(taskInfo -> {
            if (taskInfo.getState() == TitusTaskState.RUNNING) {
                return isHostOnAgentCluster(taskInfo.getHost(), agentClusterOne);
            }
            return false;
        }, "are on agent cluster one");
        assertThat(tasks).haveExactly(2, areRunningOnAgentClusterOne);
    }

    private TaskExecutorHolder waitForTaskMigration(TitusTaskInfo taskInfo) throws InterruptedException {
        TaskExecutorHolder taskHolder = jobRunner.getTaskExecutorHolders().takeNext(20, TimeUnit.SECONDS);
        if (taskHolder == null) {
            return null;
        }

        assertThat(taskInfo.getId()).isNotEqualToIgnoringCase(taskHolder.getTaskId());
        assertThat(taskHolder.getJobId()).isEqualTo(taskInfo.getJobId());

        return taskHolder;
    }

    private boolean isHostOnAgentCluster(String host, SimulatedTitusAgentCluster agentCluster) {
        List<SimulatedTitusAgent> agents = agentCluster.getAgents();
        return agents.stream().anyMatch(agent -> host.equalsIgnoreCase(agent.getHostName()));
    }

    private void setActiveCluster(List<String> clusterNames) {
        Set<String> toDeactivate = agentClient.getInstanceGroups(Empty.getDefaultInstance()).getAgentInstanceGroupsList().stream()
                .map(com.netflix.titus.grpc.protogen.AgentInstanceGroup::getId)
                .collect(Collectors.toSet());
        toDeactivate.removeAll(clusterNames);

        // Activate the new ones
        clusterNames.forEach(clusterName ->
                agentClient.updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate.newBuilder()
                        .setInstanceGroupId(clusterName)
                        .setDetail("Activating a new instance group")
                        .setLifecycleState(InstanceGroupLifecycleState.Active)
                        .build()
                )
        );

        // Deactivate the old ones
        toDeactivate.forEach(clusterName ->
                agentClient.updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate.newBuilder()
                        .setInstanceGroupId(clusterName)
                        .setDetail("Deactivating the old instance groups")
                        .setLifecycleState(InstanceGroupLifecycleState.Inactive)
                        .build()
                )
        );

        List<AgentInstanceGroup> agentServerGroups = agentClient.getInstanceGroups(Empty.getDefaultInstance()).getAgentInstanceGroupsList();

        List<String> activatedClusterNames = agentServerGroups.stream()
                .filter(g -> g.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Active)
                .map(AgentInstanceGroup::getId)
                .collect(Collectors.toList());
        assertThat(activatedClusterNames).containsAll(clusterNames);

        List<String> dectivatedClusterNames = agentServerGroups.stream()
                .filter(g -> g.getLifecycleStatus().getState() == InstanceGroupLifecycleState.Inactive)
                .map(AgentInstanceGroup::getId)
                .collect(Collectors.toList());
        assertThat(dectivatedClusterNames).containsAll(toDeactivate);
    }
}
