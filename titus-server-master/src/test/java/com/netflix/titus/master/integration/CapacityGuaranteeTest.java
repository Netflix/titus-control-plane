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

package com.netflix.titus.master.integration;

import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.testkit.client.TitusMasterClient;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.model.SimulatedAgentGroupDescriptor;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusMasterResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.master.endpoint.v2.rest.Representation2ModelConvertions.asRepresentation;
import static com.netflix.titus.testkit.data.core.ApplicationSlaSample.fromAwsInstanceType;
import static com.netflix.titus.testkit.embedded.cell.master.EmbeddedTitusMasters.basicMaster;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;

/**
 * Tests that capacity guarantees are enforced during task scheduling.
 */
@Category(IntegrationTest.class)
public class CapacityGuaranteeTest extends BaseIntegrationTest {

    private static final JobDescriptor<BatchJobExt> BATCH_JOB_8CPU = oneTaskBatchJobDescriptor()
            .but(jd -> jd.getContainer().but(c -> c.toBuilder().withContainerResources(
                    ContainerResources.newBuilder()
                            .withCpu(8)
                            .withMemoryMB(1)
                            .withDiskMB(1)
                            .withNetworkMbps(1)
                            .build()
            )))
            .but(jd -> jd.getExtensions().toBuilder().withSize(3));

    private static final ApplicationSLA CRITICAL1_GUARANTEE = fromAwsInstanceType(Tier.Critical, "c1", AwsInstanceType.M4_4XLarge, 1);
    private static final ApplicationSLA CRITICAL2_GUARANTEE = fromAwsInstanceType(Tier.Critical, "c2", AwsInstanceType.M4_4XLarge, 1);

    private final TitusMasterResource titusMasterResource = new TitusMasterResource(
            basicMaster(
                    new SimulatedCloud().createAgentInstanceGroups(
                            SimulatedAgentGroupDescriptor.awsInstanceGroup("critical1", AwsInstanceType.M4_4XLarge, 2, 2, 2)
                    )
            ).toBuilder().withProperty("titus.scheduler.globalTaskLaunchingConstraintEvaluatorEnabled", "false").build()
    );

    private InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusMasterResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusMasterResource).around(instanceGroupsScenarioBuilder);

    private JobsScenarioBuilder jobsScenarioBuilder;

    private TitusMasterClient v2Client;

    @Before
    public void setUp() throws Exception {
        jobsScenarioBuilder = new JobsScenarioBuilder(titusMasterResource.getOperations());

        instanceGroupsScenarioBuilder.synchronizeWithCloud()
                .apply("critical1", g -> g.tier(Tier.Critical).lifecycleState(InstanceGroupLifecycleState.Active));

        v2Client = titusMasterResource.getMaster().getClient();

        // Setup capacity guarantees for tiers
        v2Client.addApplicationSLA(
                asRepresentation(CRITICAL1_GUARANTEE)).
                toBlocking().
                first();
        v2Client.addApplicationSLA(
                asRepresentation(CRITICAL2_GUARANTEE)).
                toBlocking().
                first();
    }

    @Test
    public void testGuaranteesAreEnforcedInCriticalTier() throws Exception {
        JobDescriptor<BatchJobExt> c1Job = BATCH_JOB_8CPU.toBuilder().withCapacityGroup("c1").build();
        JobDescriptor<BatchJobExt> c2Job = BATCH_JOB_8CPU.toBuilder().withCapacityGroup("c2").build();

        // Run c1 job, and make sure it takes up to 16 CPUs
        jobsScenarioBuilder.schedule(c1Job, jobScenarioBuilder -> jobScenarioBuilder
                .expectTasksOnAgents(2)
                .assertTasks(tasks -> tasks.stream().filter(CapacityGuaranteeTest::isScheduled).count() == 2)
        );

        // Run c2 job, and make sure it can take its share
        jobsScenarioBuilder.schedule(c2Job, jobScenarioBuilder -> jobScenarioBuilder
                .expectTasksOnAgents(2)
                .assertTasks(tasks -> tasks.stream().filter(CapacityGuaranteeTest::isScheduled).count() == 2)
        );
    }

    private static boolean isScheduled(Task task) {
        return task.getStatus().getState() != TaskState.Accepted;
    }
}