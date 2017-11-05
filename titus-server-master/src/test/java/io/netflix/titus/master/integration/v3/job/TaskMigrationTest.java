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

package io.netflix.titus.master.integration.v3.job;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.Empty;
import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc.AgentManagementServiceBlockingStub;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TaskStatus;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import io.netflix.titus.master.integration.v3.scenario.TaskScenarioBuilder;
import io.netflix.titus.testkit.client.TitusMasterClient;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgent;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.moveToState;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startTasksInNewJob;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioTemplates.startV2TasksInNewJob;
import static io.netflix.titus.master.integration.v3.scenario.ScenarioUtil.baseServiceJobDescriptor;
import static io.netflix.titus.runtime.endpoint.v3.grpc.TaskAttributes.TASK_ATTRIBUTES_AGENT_HOST;
import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class TaskMigrationTest {

    private static final SimulatedCloud simulatedCloud = new SimulatedCloud();

    private static final SimulatedTitusAgentCluster agentClusterOne = aTitusAgentCluster("agentClusterOne", 0)
            .withComputeResources(simulatedCloud.getComputeResources())
            .withSize(3)
            .withInstanceType(AwsInstanceType.R3_8XLarge)
            .build();
    private static final SimulatedTitusAgentCluster agentClusterTwo = aTitusAgentCluster("agentClusterTwo", 1)
            .withComputeResources(simulatedCloud.getComputeResources())
            .withSize(3)
            .withInstanceType(AwsInstanceType.R3_8XLarge)
            .build();

    @ClassRule
    public static final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withSimulatedCloud(simulatedCloud)
                    .withProperty("titus.master.grpcServer.v3EnabledApps", "v3App")
                    .withProperty("titusMaster.jobManager.taskInLaunchedStateTimeoutMs", "2000")
                    .withProperty("titusMaster.jobManager.batchTaskInStartInitiatedStateTimeoutMs", "2000")
                    .withProperty("titusMaster.jobManager.serviceTaskInStartInitiatedStateTimeoutMs", "2000")
                    .withProperty("titusMaster.jobManager.taskInKillInitiatedStateTimeoutMs", "100")
                    .withProperty("titus.master.taskMigration.schedulerDelayInMillis", "100")
                    .withProperty("titus.scheduler.tierSlaUpdateIntervalMs", "10")
                    .withProperty("titus.master.capacityManagement.availableCapacityUpdateIntervalMs", "10")
                    .withCriticalTier(0.1, AwsInstanceType.M4_4XLarge)
                    .withFlexTier(0.1, AwsInstanceType.R3_8XLarge, AwsInstanceType.G2_2XLarge)
                    .withAgentCluster(agentClusterOne)
                    .withAgentCluster(agentClusterTwo)
                    .build()
    );

    private static JobsScenarioBuilder jobsScenarioBuilder;
    private static TitusMasterClient client;
    private static AgentManagementServiceBlockingStub agentClient;

    @BeforeClass
    public static void setUp() throws Exception {
        jobsScenarioBuilder = new JobsScenarioBuilder(titusMasterResource.getOperations());
        client = titusMasterResource.getMaster().getClient();
        agentClient = titusMasterResource.getOperations().getV3BlockingGrpcAgentClient();
    }

    @Test(timeout = 30_000)
    public void migrateV2ServiceJob() throws Exception {
        setActiveCluster(singletonList(agentClusterOne.getName()));
        JobDescriptor<ServiceJobExt> jobDescriptor = baseServiceJobDescriptor(true).build();
        TaskMigrationTest.jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(startV2TasksInNewJob())
                .assertEachTask(task -> isHostOnAgentCluster(task.getTaskContext().get(TASK_ATTRIBUTES_AGENT_HOST), agentClusterOne),
                        "Task should be on agentClusterOne")
                .andThen(() -> setActiveCluster(singletonList(agentClusterTwo.getName())))
                .expectTaskInSlot(0, 1)
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .allTasks(moveToState(TaskStatus.TaskState.Started))
                .assertEachTask(task -> isHostOnAgentCluster(task.getTaskContext().get(TASK_ATTRIBUTES_AGENT_HOST), agentClusterTwo),
                        "Task should be on agentClusterTwo")
        );
    }

    @Test(timeout = 30_000)
    public void migrateV3ServiceJob() throws Exception {
        setActiveCluster(singletonList(agentClusterOne.getName()));
        JobDescriptor<ServiceJobExt> jobDescriptor = baseServiceJobDescriptor(false).build();
        TaskMigrationTest.jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(startTasksInNewJob())
                .assertEachTask(task -> isHostOnAgentCluster(task.getTaskContext().get(TASK_ATTRIBUTES_AGENT_HOST), agentClusterOne),
                        "Task should be on agentClusterOne")
                .andThen(() -> setActiveCluster(singletonList(agentClusterTwo.getName())))
                .expectTaskInSlot(0, 1)
                .allTasks(TaskScenarioBuilder::expectTaskOnAgent)
                .allTasks(moveToState(TaskStatus.TaskState.Started))
                .assertEachTask(task -> isHostOnAgentCluster(task.getTaskContext().get(TASK_ATTRIBUTES_AGENT_HOST), agentClusterTwo),
                        "Task should be on agentClusterTwo")
        );
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
        List<String> activatedClusterNames = agentServerGroups.stream().map(AgentInstanceGroup::getId).collect(Collectors.toList());

        assertThat(activatedClusterNames).contains(clusterNames.toArray(new String[clusterNames.size()]));
    }

    private boolean isHostOnAgentCluster(String host, SimulatedTitusAgentCluster agentCluster) {
        List<SimulatedTitusAgent> agents = agentCluster.getAgents();
        return agents.stream().anyMatch(agent -> host.equalsIgnoreCase(agent.getHostName()));
    }
}
