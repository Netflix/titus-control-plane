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

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleState;
import com.netflix.titus.grpc.protogen.InstanceGroupLifecycleStateUpdate;
import com.netflix.titus.grpc.protogen.TierUpdate;
import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.endpoint.v2.V2TitusDataGenerator;
import io.netflix.titus.master.endpoint.v2.rest.representation.TitusJobSpec;
import io.netflix.titus.testkit.client.TitusMasterClient;
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

import static io.netflix.titus.master.endpoint.v2.rest.Representation2ModelConvertions.asRepresentation;
import static io.netflix.titus.testkit.data.core.ApplicationSlaSample.fromAwsInstanceType;
import static io.netflix.titus.testkit.embedded.cloud.agent.SimulatedTitusAgentCluster.aTitusAgentCluster;
import static junit.framework.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that capacity guarantees are enforced during task scheduling.
 */
@Category(IntegrationTest.class)
public class CapacityGuaranteeTest {

    private static final ApplicationSLA CRITICAL1_GUARANTEE = fromAwsInstanceType(Tier.Critical, "c1", AwsInstanceType.M4_4XLarge, 1);
    private static final ApplicationSLA CRITICAL2_GUARANTEE = fromAwsInstanceType(Tier.Critical, "c2", AwsInstanceType.M4_4XLarge, 1);

    @Rule
    public final TitusMasterResource titusMasterResource = new TitusMasterResource(
            EmbeddedTitusMaster.testTitusMaster()
                    .withProperty("titus.scheduler.tierSlaUpdateIntervalMs", "10")
                    .withProperty("titus.master.capacityManagement.availableCapacityUpdateIntervalMs", "10")
                    .withCriticalTier(0.1, AwsInstanceType.M4_4XLarge)
                    .withFlexTier(0.1, AwsInstanceType.R3_4XLarge)
                    .withAgentCluster(aTitusAgentCluster("criticalAgentCluster", 0).withSize(2).withInstanceType(AwsInstanceType.M4_4XLarge))
                    .withAgentCluster(aTitusAgentCluster("flexAgentCluster", 1).withSize(2).withInstanceType(AwsInstanceType.R3_4XLarge))
                    .build()
    );

    private EmbeddedTitusMaster titusMaster;

    private TitusMasterClient client;
    private ExtTestSubscriber<TaskExecutorHolder> taskExecutorHolders;

    private final V2TitusDataGenerator generator = new V2TitusDataGenerator();
    private JobRunner jobRunner;

    @Before
    public void setUp() throws Exception {
        titusMaster = titusMasterResource.getMaster();

        client = titusMaster.getClient();
        jobRunner = new JobRunner(titusMaster);

        taskExecutorHolders = new ExtTestSubscriber<>();
        titusMaster.observeLaunchedTasks().subscribe(taskExecutorHolders);

        // Configure Clusters
        titusMasterResource.getOperations().getV3BlockingGrpcAgentClient().updateInstanceGroupTier(
                TierUpdate.newBuilder()
                        .setInstanceGroupId("criticalAgentCluster")
                        .setTier(com.netflix.titus.grpc.protogen.Tier.Critical)
                        .build()
        );
        titusMasterResource.getOperations().getV3BlockingGrpcAgentClient().updateInstanceGroupLifecycleState(
                InstanceGroupLifecycleStateUpdate.newBuilder()
                        .setInstanceGroupId("criticalAgentCluster")
                        .setDetail("activate")
                        .setLifecycleState(InstanceGroupLifecycleState.Active)
                        .build()
        );
        titusMasterResource.getOperations().getV3BlockingGrpcAgentClient().updateInstanceGroupLifecycleState(
                InstanceGroupLifecycleStateUpdate.newBuilder()
                        .setInstanceGroupId("flexAgentCluster")
                        .setDetail("activate")
                        .setLifecycleState(InstanceGroupLifecycleState.Active)
                        .build()
        );

        // Setup capacity guarantees for tiers
        client.addApplicationSLA(asRepresentation(CRITICAL1_GUARANTEE)).toBlocking().first();
        client.addApplicationSLA(asRepresentation(CRITICAL2_GUARANTEE)).toBlocking().first();
    }

    @Test
    @Ignore
    public void testGuaranteesAreEnforcedInCriticalTier() throws Exception {
        // FIXME Remove this once we have notification mechanism
        Thread.sleep(500);

        // Run c1 job, and make sure it takes up to 16 CPUs
        try {
            TitusJobSpec c1Job = new TitusJobSpec.Builder(generator.createBatchJob("c1"))
                    .appName("c1")
                    .cpu(8)
                    .memory(1)
                    .networkMbps(1)
                    .instances(3)
                    .build();
            jobRunner.runJob(c1Job);
            fail("Expected to fail, as not all tasks can be run within the available capacity");
        } catch (AssertionError ignore) {
            // We expect it to fail, as not all tasks can be run
        }

        TitusJobInfo c1Job = client.findJobs().toBlocking().first();
        assertThat(c1Job.getTasks()).hasSize(3);
        List<TaskInfo> c1Running = c1Job.getTasks().stream().filter(t -> t.getState() == TitusTaskState.RUNNING).collect(Collectors.toList());
        assertThat(c1Running).hasSize(2);
        List<TaskInfo> c1Queued = c1Job.getTasks().stream().filter(t -> t.getState() == TitusTaskState.QUEUED).collect(Collectors.toList());
        assertThat(c1Queued).hasSize(1);

        // Run c2 job, and make sure it can take its share
        TitusJobSpec c2Job = new TitusJobSpec.Builder(generator.createBatchJob("c2"))
                .appName("c2")
                .cpu(1)
                .memory(1024)
                .networkMbps(100)
                .instances(16)
                .build();
        jobRunner.runJob(c2Job);
    }
}