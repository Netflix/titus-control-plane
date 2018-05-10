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

package com.netflix.titus.master.scheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.aws.AwsInstanceType;
import com.netflix.titus.master.scheduler.constraint.GlobalAgentClusterConstraint;
import com.netflix.titus.testkit.model.agent.AgentDeployment;
import org.apache.mesos.Protos;
import org.assertj.core.api.Assertions;
import org.eclipse.jetty.util.HostMap;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GlobalAgentClusterConstraintTest {
    private final SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);
    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);
    private final AgentStatusMonitor agentStatusMonitor = mock(AgentStatusMonitor.class);

    private final TaskRequest nonGpuTaskRequest = createTaskRequest(0);
    private final TaskRequest gpuTaskRequest = createTaskRequest(4);

    @Before
    public void setUp() throws Exception {
        when(schedulerConfiguration.getInstanceGroupAttributeName()).thenReturn("asg");
        when(schedulerConfiguration.getInstanceAttributeName()).thenReturn("id");
    }

    @Test
    public void testGpuTaskWithP2Cluster() {
        AgentDeployment deployment = AgentDeployment.newDeployment().withActiveInstanceGroup(Tier.Flex, "f1", AwsInstanceType.P2_8XLarge, 1).build();
        expectSuccess(deployment, gpuTaskRequest, AwsInstanceType.P2_8XLarge);
    }

    @Test
    public void testGpuTaskWithM4Cluster() {
        AgentDeployment deployment = AgentDeployment.newDeployment().withActiveInstanceGroup(Tier.Flex, "f1", AwsInstanceType.M4_4XLarge, 1).build();
        expectFailure(deployment, gpuTaskRequest, AwsInstanceType.M4_4XLarge);
    }

    @Test
    public void testGpuTaskWithM4AndP2Clusters() {
        AgentDeployment deployment = AgentDeployment.newDeployment()
                .withActiveInstanceGroup(Tier.Flex, "f1", AwsInstanceType.M4_4XLarge, 1)
                .withActiveInstanceGroup(Tier.Flex, "f2", AwsInstanceType.P2_8XLarge, 1)
                .build();
        expectSuccess(deployment, gpuTaskRequest, AwsInstanceType.P2_8XLarge);
    }

    @Test
    public void testNonGpuTaskWithP2Clusters() {
        AgentDeployment deployment = AgentDeployment.newDeployment().withActiveInstanceGroup(Tier.Flex, "f1", AwsInstanceType.P2_8XLarge, 1).build();
        expectFailure(deployment, nonGpuTaskRequest, AwsInstanceType.P2_8XLarge);
    }

    @Test
    public void testNonGpuTaskWithM4Cluster() {
        AgentDeployment deployment = AgentDeployment.newDeployment().withActiveInstanceGroup(Tier.Flex, "f1", AwsInstanceType.M4_4XLarge, 1).build();
        expectSuccess(deployment, nonGpuTaskRequest, AwsInstanceType.M4_4XLarge);
    }

    @Test
    public void testNonGpuTaskWithP2AndM4AndR4Clusters() {
        AgentDeployment deployment = AgentDeployment.newDeployment()
                .withActiveInstanceGroup(Tier.Flex, "f1", AwsInstanceType.M4_4XLarge, 1)
                .withActiveInstanceGroup(Tier.Flex, "f2", AwsInstanceType.P2_8XLarge, 1)
                .withActiveInstanceGroup(Tier.Flex, "f3", AwsInstanceType.R3_8XLarge, 1)
                .build();
        expectSuccess(deployment, nonGpuTaskRequest, AwsInstanceType.M4_4XLarge);
    }

    private void expectSuccess(AgentDeployment agentDeployment, TaskRequest taskRequest, AwsInstanceType instanceType) {
        Assertions.assertThat(evaluate(agentDeployment, taskRequest, instanceType)).isTrue();
    }

    private void expectFailure(AgentDeployment agentDeployment, TaskRequest taskRequest, AwsInstanceType instanceType) {
        Assertions.assertThat(evaluate(agentDeployment, taskRequest, instanceType)).isFalse();
    }

    private boolean evaluate(AgentDeployment agentDeployment, TaskRequest taskRequest, AwsInstanceType instanceType) {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(agentDeployment);
        globalConstraint.prepare();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(taskRequest, createVirtualMachine(agentDeployment, instanceType), null);
        return result.isSuccessful();
    }

    private GlobalAgentClusterConstraint createGlobalConstraint(AgentDeployment agentDeployment) {
        when(agentManagementService.getInstanceGroup(anyString())).thenAnswer(argument ->
                agentDeployment.getInstanceGroup(argument.getArgument(0))
        );
        when(agentStatusMonitor.isHealthy(anyString())).thenReturn(true);
        return new GlobalAgentClusterConstraint(schedulerConfiguration, agentManagementService, agentStatusMonitor);
    }

    private VirtualMachineCurrentState createVirtualMachine(AgentDeployment agentDeployment, AwsInstanceType instanceType) {
        AgentInstanceGroup instanceGroup = agentDeployment.getInstanceGroupsOfType(instanceType.name()).get(0);
        AgentInstance instance = agentDeployment.getInstances(instanceGroup.getId()).get(0);

        final long now = System.currentTimeMillis();
        final Map<String, Protos.Attribute> attributeMap = new HostMap<>();
        attributeMap.put("asg", Protos.Attribute.newBuilder().setName("asg")
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(instanceGroup.getId())).build());
        attributeMap.put("id", Protos.Attribute.newBuilder().setName("id")
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(instance.getId())).build());
        attributeMap.put(
                "itype",
                Protos.Attribute.newBuilder().setName("itype")
                        .setType(Protos.Value.Type.TEXT)
                        .setText(Protos.Value.Text.newBuilder().setValue(instanceGroup.getInstanceType()))
                        .build()
        );

        ResourceDimension instanceResources = instanceGroup.getResourceDimension();
        final Map<String, Double> scalars = new HostMap<>();
        scalars.put("gpu", (double) instanceResources.getGpu());
        final VirtualMachineLease lease = new VirtualMachineLease() {
            @Override
            public String getId() {
                return "123";
            }

            @Override
            public long getOfferedTime() {
                return now;
            }

            @Override
            public String hostname() {
                return instance.getHostname();
            }

            @Override
            public String getVMID() {
                return "vmid";
            }

            @Override
            public double cpuCores() {
                return instanceResources.getCpu();
            }

            @Override
            public double memoryMB() {
                return instanceResources.getMemoryMB();
            }

            @Override
            public double networkMbps() {
                return instanceResources.getNetworkMbs();
            }

            @Override
            public double diskMB() {
                return instanceResources.getDiskMB();
            }

            @Override
            public List<Range> portRanges() {
                return null;
            }

            @Override
            public Protos.Offer getOffer() {
                return null;
            }

            @Override
            public Map<String, Protos.Attribute> getAttributeMap() {
                return attributeMap;
            }

            @Override
            public Double getScalarValue(String name) {
                return scalars.get(name);
            }

            @Override
            public Map<String, Double> getScalarValues() {
                return scalars;
            }
        };
        return new VirtualMachineCurrentState() {
            @Override
            public String getHostname() {
                return instance.getHostname();
            }

            @Override
            public Map<String, PreferentialNamedConsumableResourceSet> getResourceSets() {
                return null;
            }

            @Override
            public VirtualMachineLease getCurrAvailableResources() {
                return lease;
            }

            @Override
            public Collection<Protos.Offer> getAllCurrentOffers() {
                return null;
            }

            @Override
            public Collection<TaskAssignmentResult> getTasksCurrentlyAssigned() {
                return Collections.emptyList();
            }

            @Override
            public Collection<TaskRequest> getRunningTasks() {
                return Collections.emptyList();
            }

            @Override
            public long getDisabledUntil() {
                return 0;
            }
        };
    }

    private TaskRequest createTaskRequest(int gpu) {
        final Map<String, Double> scalars = new HostMap<>();
        scalars.put("gpu", (double) gpu);
        final QAttributes qAttributes = new QAttributes.QAttributesAdaptor(1, "foo");
        return new QueuableTask() {
            @Override
            public QAttributes getQAttributes() {
                return qAttributes;
            }

            @Override
            public String getId() {
                return "task1";
            }

            @Override
            public String taskGroupName() {
                return null;
            }

            @Override
            public double getCPUs() {
                return 1.0;
            }

            @Override
            public double getMemory() {
                return 1000.0;
            }

            @Override
            public double getNetworkMbps() {
                return 0;
            }

            @Override
            public double getDisk() {
                return 0;
            }

            @Override
            public int getPorts() {
                return 0;
            }

            @Override
            public Map<String, Double> getScalarRequests() {
                return scalars;
            }

            @Override
            public Map<String, NamedResourceSetRequest> getCustomNamedResources() {
                return null;
            }

            @Override
            public List<? extends ConstraintEvaluator> getHardConstraints() {
                return null;
            }

            @Override
            public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
                return null;
            }

            @Override
            public void setAssignedResources(AssignedResources assignedResources) {
            }

            @Override
            public AssignedResources getAssignedResources() {
                return null;
            }
        };
    }
}