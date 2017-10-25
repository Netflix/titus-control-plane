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

import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import io.netflix.titus.api.model.event.AutoScaleEvent;
import io.netflix.titus.api.model.event.ScaleUpEvent;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import io.netflix.titus.testkit.embedded.master.EmbeddedTitusMaster;
import io.netflix.titus.testkit.junit.category.IntegrationTest;
import io.netflix.titus.testkit.junit.master.TitusMasterResource;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class AutoScalingTest extends BaseIntegrationTest {

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withProperty("titus.agent.autoScaleRuleMinIdleToKeep", "5")
                    .withProperty("titus.agent.autoScaleRuleMaxIdleToKeep", "10")
                    .withProperty("titus.agent.autoScaleRuleCoolDownSec", "5")
                    .withAgentCluster(
                            aTitusAgentCluster("agentClusterOne", 0)
                                    .withInstanceType(AwsInstanceType.M3_XLARGE)
                                    .withSize(2)
                                    .withMinIdleHostsToKeep(5)
                                    .withMaxIdleHostsToKeep(10)
                                    .withCoolDownSec(1)
                    )
                    .withCriticalTier(0.1, AwsInstanceType.M3_XLARGE)
                    .withFlexTier(0.1, AwsInstanceType.M3_XLARGE)
                    .build()
    );

    private EmbeddedTitusMaster titusMaster;

    private ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;

    @Before
    public void setUp() throws Exception {
        titusMaster = titusMasterResource.getMaster();
        taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);

        titusMasterResource.getOperations().getV3BlockingGrpcAgentClient().updateInstanceGroupLifecycleState(
                InstanceGroupLifecycleStateUpdate.newBuilder()
                        .setInstanceGroupId("agentClusterOne")
                        .setDetail("activate")
                        .setLifecycleState(InstanceGroupLifecycleState.Active)
                        .build()
        );
    }

    @Test(timeout = 30000)
    @Ignore
    public void testAgentClustersAreScaledUpToMinIdleInstances() throws Exception {
        ExtTestSubscriber<AutoScaleEvent> scaleActionsSubscriber = new ExtTestSubscriber<>();
        titusMaster.observeAutoScaleEvents().subscribe(scaleActionsSubscriber);

        AutoScaleEvent event = scaleActionsSubscriber.takeNextOrWait();
        assertThat(event).isExactlyInstanceOf(ScaleUpEvent.class);

        ScaleUpEvent scaleUpEvent = (ScaleUpEvent) event;
        assertThat(scaleUpEvent.getRequestedSize()).isEqualTo(10); // maxIdleSize
    }
}
