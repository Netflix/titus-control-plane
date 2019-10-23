/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.integration.v4;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.model.job.ContainerResources;
import com.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.grpc.protogen.v4.Id;
import com.netflix.titus.grpc.protogen.v4.Machine;
import com.netflix.titus.grpc.protogen.v4.MachineResources;
import com.netflix.titus.grpc.protogen.v4.MachineServiceGrpc.MachineServiceBlockingStub;
import com.netflix.titus.grpc.protogen.v4.QueryRequest;
import com.netflix.titus.master.integration.BaseIntegrationTest;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupScenarioTemplates;
import com.netflix.titus.master.integration.v3.scenario.InstanceGroupsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.JobsScenarioBuilder;
import com.netflix.titus.master.integration.v3.scenario.ScenarioTemplates;
import com.netflix.titus.master.machine.endpoint.grpc.MachineResourcesEvaluator;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.junit.master.TitusStackResource;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import static com.netflix.titus.testkit.embedded.cell.EmbeddedTitusCells.basicCell;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskBatchJobDescriptor;
import static org.assertj.core.api.Assertions.assertThat;

@Category(IntegrationTest.class)
public class MachineTest extends BaseIntegrationTest {

    private final TitusStackResource titusStackResource = new TitusStackResource(basicCell(2), true);

    private final JobsScenarioBuilder jobsScenarioBuilder = new JobsScenarioBuilder(titusStackResource);

    private final InstanceGroupsScenarioBuilder instanceGroupsScenarioBuilder = new InstanceGroupsScenarioBuilder(titusStackResource);

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule(titusStackResource).around(instanceGroupsScenarioBuilder).around(jobsScenarioBuilder);

    private MachineServiceBlockingStub client;

    @Before
    public void setUp() throws Exception {
        instanceGroupsScenarioBuilder.synchronizeWithCloud().template(InstanceGroupScenarioTemplates.basicCloudActivation());

        this.client = titusStackResource.getOperations().getBlockingGrpcMachineClient();
    }

    /**
     * Schedule one task. One machine should have all resources available, the other one decremented.
     */
    @Test(timeout = 30_000)
    public void testMachineResources() throws Exception {
        JobDescriptor<BatchJobExt> jobDescriptor = oneTaskBatchJobDescriptor();
        ContainerResources containerResources = jobDescriptor.getContainer().getContainerResources();

        jobsScenarioBuilder.schedule(jobDescriptor, jobScenarioBuilder -> jobScenarioBuilder
                .template(ScenarioTemplates.startTasksInNewJob())
                .andThen(() -> {
                    List<Machine> machines = client.getMachines(QueryRequest.getDefaultInstance()).getItemsList();
                    assertThat(machines).hasSize(6);

                    // Find flex instances
                    List<Machine> flexMachines = machines.stream().filter(m -> m.getId().contains("flex1")).collect(Collectors.toList());
                    assertThat(flexMachines).hasSize(2);

                    Machine used;
                    Machine unused;
                    if (flexMachines.get(0).getAllocatedResources().equals(MachineResources.getDefaultInstance())) {
                        used = flexMachines.get(1);
                        unused = flexMachines.get(0);
                    } else {
                        used = flexMachines.get(0);
                        unused = flexMachines.get(1);
                    }

                    assertThat((double) used.getAllocatedResources().getCpu()).isEqualTo(containerResources.getCpu());
                    assertThat(used.getAllocatedResources().getMemoryMB()).isEqualTo(containerResources.getMemoryMB());
                    assertThat(used.getAllocatedResources().getDiskMB()).isEqualTo(containerResources.getDiskMB());
                    assertThat(used.getAllocatedResources().getNetworkMbps()).isEqualTo(containerResources.getNetworkMbps());

                    for (Machine machine : Arrays.asList(used, unused)) {
                        assertThat(machine.getInstanceResourcesCount()).isEqualTo(2);
                        assertThat(machine.getInstanceResources(0).getCategory()).isEqualTo(MachineResourcesEvaluator.MACHINE_RESOURCE_PHYSICAL);
                    }

                    // Now direct calls
                    for (Machine machine : Arrays.asList(used, unused)) {
                        assertThat(client.getMachine(Id.newBuilder().setId(machine.getId()).build())).isEqualTo(machine);
                    }
                })
        );
    }
}
