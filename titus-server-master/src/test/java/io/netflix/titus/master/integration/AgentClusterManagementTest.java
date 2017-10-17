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

import com.netflix.titus.grpc.protogen.AgentInstanceGroup;
import com.netflix.titus.grpc.protogen.AgentManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.Id;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.model.v2.TitusV2ModelGenerator;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A collection of tests verifying agent cluster add/remove, active/deactivate functionality.
 */
@Category(IntegrationTest.class)
public class AgentClusterManagementTest extends BaseIntegrationTest {

    private static final String FLEX_AGENT_1 = "flexAgent1";
    private static final String FLEX_AGENT_2 = "flexAgent2";

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withCriticalTier(0.1, AwsInstanceType.M3_XLARGE)
                    .withFlexTier(0.1, AwsInstanceType.M3_2XLARGE, AwsInstanceType.M4_10XLarge)
                    .withAgentCluster(aTitusAgentCluster("criticalAgent", 0).withSize(2).withInstanceType(AwsInstanceType.M3_XLARGE))
                    .withAgentCluster(aTitusAgentCluster(FLEX_AGENT_1, 1).withSize(2).withInstanceType(AwsInstanceType.M3_2XLARGE))
                    .withAgentCluster(aTitusAgentCluster(FLEX_AGENT_2, 2).withSize(2).withInstanceType(AwsInstanceType.M4_10XLarge))
                    .build()
    );

    private final TitusV2ModelGenerator generator = new TitusV2ModelGenerator();

    private ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;
    private JobRunner jobRunner;

    private AgentManagementServiceGrpc.AgentManagementServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        EmbeddedTitusMaster titusMaster = titusMasterResource.getMaster();
        client = titusMasterResource.getOperations().getV3BlockingGrpcAgentClient();
        jobRunner = new JobRunner(titusMaster);
        taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);
    }

    /**
     * Having two clusters in flex tier, disable second and verify that job is scheduled on the first one.
     * Next disable first, and enable second, and verify that scheduled job runs on the second cluster.
     */
    @Test(timeout = 30000)
    public void scheduleJobOnActiveAgentCluster() throws Exception {
        // Activate 'flexAgent1' only
        changeServerGroupLifecycleState(FLEX_AGENT_1, InstanceGroupLifecycleState.Active);

        TaskExecutorHolder flex1Holder = jobRunner.runJob(generator.newJobSpec(TitusJobType.batch, "flex1Job")).get(0);
        assertThat(flex1Holder.getInstanceType()).isEqualTo(AwsInstanceType.M3_2XLARGE);

        flex1Holder.transitionTo(Protos.TaskState.TASK_FINISHED);

        // Activate 'flexAgent2' only
        changeServerGroupLifecycleState(FLEX_AGENT_1, InstanceGroupLifecycleState.Inactive);
        changeServerGroupLifecycleState(FLEX_AGENT_2, InstanceGroupLifecycleState.Active);

        TaskExecutorHolder flex2Holder = jobRunner.runJob(generator.newJobSpec(TitusJobType.batch, "flex2Job")).get(0);
        assertThat(flex2Holder.getInstanceType()).isEqualTo(AwsInstanceType.M4_10XLarge);

        flex2Holder.transitionTo(Protos.TaskState.TASK_FINISHED);
    }

    private void changeServerGroupLifecycleState(String instanceGroupName, InstanceGroupLifecycleState state) {
        client.updateInstanceGroupLifecycleState(InstanceGroupLifecycleStateUpdate.newBuilder()
                .setInstanceGroupId(instanceGroupName)
                .setDetail("Activating a new instance group")
                .setLifecycleState(state)
                .build()
        );

        AgentInstanceGroup changed = client.getInstanceGroup(Id.newBuilder().setId(instanceGroupName).build());
        assertThat(changed.getLifecycleStatus().getState()).isEqualTo(state);
    }
}
