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

package io.netflix.titus.master.scheduler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceSet;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.ConfigurationMockSamples;
import io.netflix.titus.master.scheduler.constraint.GlobalAgentClusterConstraint;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import org.apache.mesos.Protos;
import org.eclipse.jetty.util.HostMap;
import org.junit.Assert;
import org.junit.Test;

import static io.netflix.titus.common.aws.AwsInstanceType.M4_4XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.P2_8XLARGE_ID;
import static io.netflix.titus.common.aws.AwsInstanceType.R4_8XLARGE_ID;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GlobalAgentClusterConstraintTest {

    private static final String agentClusterAttributeName = ConfigurationMockSamples.agentClusterAttributeName;

    @Test
    public void testGpuTaskWithM4AndP2Clusters() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(Arrays.asList(
                Pair.of(Tier.Critical, M4_4XLARGE_ID),
                Pair.of(Tier.Flex, P2_8XLARGE_ID)
        ));
        globalConstraint.prepare();
        final TaskRequest task = createGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createGpuAgentCurrentState("hostA", P2_8XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
    }

    @Test
    public void testNonGpuTaskWithM4AndP2Clusters() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(Arrays.asList(
                Pair.of(Tier.Critical, M4_4XLARGE_ID),
                Pair.of(Tier.Flex, P2_8XLARGE_ID)
        ));
        globalConstraint.prepare();
        final TaskRequest task = createNonGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createGpuAgentCurrentState("hostA", P2_8XLARGE_ID), null);
        Assert.assertFalse(result.isSuccessful());
    }

    @Test
    public void testGpuTaskWithM4Cluster() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(Collections.singletonList(
                Pair.of(Tier.Critical, M4_4XLARGE_ID)
        ));
        globalConstraint.prepare();
        final TaskRequest task = createGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("hostB", M4_4XLARGE_ID), null);
        Assert.assertFalse(result.isSuccessful());
    }

    @Test
    public void testNonGpuTaskWithM4Cluster() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(Collections.singletonList(
                Pair.of(Tier.Flex, M4_4XLARGE_ID)
        ));
        globalConstraint.prepare();
        final TaskRequest task = createNonGpuTaskRequest();
        final ConstraintEvaluator.Result result;
        result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("hostB", M4_4XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
    }

    @Test
    public void testGpuTaskWithP2Cluster() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(Collections.singletonList(
                Pair.of(Tier.Flex, P2_8XLARGE_ID)
        ));
        globalConstraint.prepare();
        final TaskRequest task = createGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createGpuAgentCurrentState("hostB", P2_8XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
    }

    @Test
    public void testNonGpuTaskWithM4R4P2Clusters() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(Arrays.asList(
                Pair.of(Tier.Critical, M4_4XLARGE_ID),
                Pair.of(Tier.Flex, R4_8XLARGE_ID),
                Pair.of(Tier.Flex, P2_8XLARGE_ID)
        ));
        globalConstraint.prepare();
        // task uses tier 1, so it will expect to land on r4.
        final TaskRequest task = createNonGpuTaskRequest();
        ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("R4HostA", R4_8XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
        // since tier 1 is set to go to m4, the following will fail for m3s.
        result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("M4HostX", M4_4XLARGE_ID), null);
        Assert.assertFalse(result.isSuccessful());
    }

    private GlobalAgentClusterConstraint createGlobalConstraint(List<Pair<Tier, String>> instanceTypePairs) {
        SchedulerConfiguration schedulerConfiguration = mock(SchedulerConfiguration.class);
        when(schedulerConfiguration.getInstanceGroupAttributeName()).thenReturn("asg");

        AgentManagementService agentManagementService = mock(AgentManagementService.class);
        when(agentManagementService.getInstanceGroup(anyString())).thenAnswer(argument -> getInstanceGroups(instanceTypePairs).stream()
                .filter(instanceGroup -> instanceGroup.getId().equals(argument.getArgument(0)))
                .findFirst()
                .orElse(null));
        return new GlobalAgentClusterConstraint(schedulerConfiguration, agentManagementService);
    }

    private List<AgentInstanceGroup> getInstanceGroups(List<Pair<Tier, String>> instanceTypePairs) {
        return instanceTypePairs.stream()
                .map(instanceTypePair -> {
                    String instanceType = instanceTypePair.getRight();
                    String id = instanceType + "-v000";
                    return AgentGenerator.agentServerGroups(instanceTypePair.getLeft(), 1).getValue()
                            .toBuilder()
                            .withId(id)
                            .withInstanceType(instanceType)
                            .build();
                })
                .collect(Collectors.toList());
    }

    private VirtualMachineCurrentState createGpuAgentCurrentState(final String hostname, final String clusterAttrVal) {
        final long now = System.currentTimeMillis();
        final Map<String, Protos.Attribute> attributeMap = new HostMap<>();
        attributeMap.put(agentClusterAttributeName, Protos.Attribute.newBuilder().setName(agentClusterAttributeName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(clusterAttrVal)).build());
        attributeMap.put("asg", Protos.Attribute.newBuilder().setName("asg")
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(P2_8XLARGE_ID + "-v000")).build());
        attributeMap.put(
                "itype",
                Protos.Attribute.newBuilder().setName("itype")
                        .setType(Protos.Value.Type.TEXT)
                        .setText(Protos.Value.Text.newBuilder().setValue(P2_8XLARGE_ID))
                        .build()
        );
        final Map<String, Double> scalars = new HostMap<>();
        scalars.put("gpu", 4.0);
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
                return hostname;
            }

            @Override
            public String getVMID() {
                return "vmid";
            }

            @Override
            public double cpuCores() {
                return 4;
            }

            @Override
            public double memoryMB() {
                return 4000;
            }

            @Override
            public double networkMbps() {
                return 1024;
            }

            @Override
            public double diskMB() {
                return 1000;
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
                return hostname;
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

    private VirtualMachineCurrentState createNonGpuAgentCurrentState(final String hostname, final String clusterAttrVal) {
        final long now = System.currentTimeMillis();
        final Map<String, Protos.Attribute> attributeMap = new HostMap<>();
        attributeMap.put(agentClusterAttributeName, Protos.Attribute.newBuilder().setName(agentClusterAttributeName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(clusterAttrVal)).build());
        attributeMap.put("asg", Protos.Attribute.newBuilder().setName("asg")
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(clusterAttrVal + "-v000")).build());
        attributeMap.put(
                "itype",
                Protos.Attribute.newBuilder().setName("itype")
                        .setType(Protos.Value.Type.TEXT)
                        .setText(Protos.Value.Text.newBuilder().setValue(clusterAttrVal))
                        .build()
        );
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
                return hostname;
            }

            @Override
            public String getVMID() {
                return "vmid";
            }

            @Override
            public double cpuCores() {
                return 4;
            }

            @Override
            public double memoryMB() {
                return 4000;
            }

            @Override
            public double networkMbps() {
                return 1024;
            }

            @Override
            public double diskMB() {
                return 1000;
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
                return null;
            }

            @Override
            public Map<String, Double> getScalarValues() {
                return null;
            }
        };
        return new VirtualMachineCurrentState() {
            @Override
            public String getHostname() {
                return hostname;
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

    private TaskRequest createGpuTaskRequest() {
        final Map<String, Double> scalars = new HostMap<>();
        scalars.put("gpu", 1.0);
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

    private TaskRequest createNonGpuTaskRequest() {
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
                return null;
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