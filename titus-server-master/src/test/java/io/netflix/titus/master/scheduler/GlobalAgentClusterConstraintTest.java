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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.ConfigurationMockSamples;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.scheduler.constraint.GlobalAgentClusterConstraint;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import org.apache.mesos.Protos;
import org.eclipse.jetty.util.HostMap;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class GlobalAgentClusterConstraintTest {

    private static final String agentClusterAttributeName = ConfigurationMockSamples.agentClusterAttributeName;

    private CapacityManagementConfiguration getCapMgmtConfig(String... instanceTypes) {
        return new CapacityManagementConfiguration() {

            @Override
            public Map<String, InstanceTypeConfig> getInstanceTypes() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, TierConfig> getTiers() {
                Map<String, TierConfig> tiers = new HashMap<>();
                int i = 0;
                for (String s : instanceTypes) {
                    tiers.put(
                            Integer.toString(i++),
                            new TierConfig() {
                                @Override
                                public String[] getInstanceTypes() {
                                    return new String[]{s};
                                }

                                @Override
                                public double getBuffer() {
                                    return 0.25;
                                }
                            }
                    );
                }
                return tiers;
            }

            @Override
            public ResourceDimensionConfiguration getDefaultApplicationResourceDimension() {
                return null;
            }

            @Override
            public int getDefaultApplicationInstanceCount() {
                return 0;
            }

            @Override
            public long getAvailableCapacityUpdateIntervalMs() {
                return 30000;
            }
        };
    }

    private GlobalAgentClusterConstraint createGlobalConstraint(CapacityManagementConfiguration config) {
        return new GlobalAgentClusterConstraint(
                ConfigurationMockSamples.withAutoScaleClusterInfo(mock(MasterConfiguration.class)),
                config
        );
    }

    @Test
    public void testGpuTaskWithM3AndG2Clusters() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(getCapMgmtConfig(AwsInstanceType.M4_4XLARGE_ID, AwsInstanceType.G2_2XLARGE_ID));
        globalConstraint.prepare();
        final TaskRequest task = createGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createGpuAgentCurrentState("hostA", AwsInstanceType.G2_2XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
    }

    @Test
    public void testNonGpuTaskWithM3AndG2Clusters() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(getCapMgmtConfig(AwsInstanceType.M4_4XLARGE_ID, AwsInstanceType.G2_2XLARGE_ID));
        globalConstraint.prepare();
        final TaskRequest task = createNonGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createGpuAgentCurrentState("hostA", AwsInstanceType.G2_2XLARGE_ID), null);
        Assert.assertFalse(result.isSuccessful());
    }

    @Test
    public void testGpuTaskWithM3Cluster() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(getCapMgmtConfig(AwsInstanceType.M4_4XLARGE_ID));
        globalConstraint.prepare();
        final TaskRequest task = createGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("hostB", AwsInstanceType.M4_4XLARGE_ID), null);
        Assert.assertFalse(result.isSuccessful());
    }

    @Test
    public void testNonGpuTaskWithM3Cluster() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(getCapMgmtConfig(AwsInstanceType.M4_4XLARGE_ID));
        globalConstraint.prepare();
        final TaskRequest task = createNonGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("hostB", AwsInstanceType.M4_4XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
    }

    @Test
    public void testGpuTaskWithG2Cluster() {
        final GlobalAgentClusterConstraint globalConstraint = createGlobalConstraint(getCapMgmtConfig(AwsInstanceType.G2_2XLARGE_ID));
        globalConstraint.prepare();
        final TaskRequest task = createGpuTaskRequest();
        final ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createGpuAgentCurrentState("hostB", AwsInstanceType.G2_2XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
    }

    @Test
    public void testNonGpuTaskWithM3C3G2Clusters() {
        final GlobalAgentClusterConstraint globalConstraint =
                createGlobalConstraint(getCapMgmtConfig(AwsInstanceType.M4_4XLARGE_ID, AwsInstanceType.R3_4XLARGE_ID, AwsInstanceType.G2_2XLARGE_ID));
        globalConstraint.prepare();
        // task uses tier 1, so it will expect to land on c3s.
        final TaskRequest task = createNonGpuTaskRequest();
        ConstraintEvaluator.Result result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("C3HostA", AwsInstanceType.R3_4XLARGE_ID), null);
        Assert.assertTrue(result.isSuccessful());
        // since tier 1 is set to go to vc3s, the following will fail for m3s.
        result = globalConstraint.evaluate(task, createNonGpuAgentCurrentState("M3HostX", AwsInstanceType.M4_4XLARGE_ID), null);
        Assert.assertFalse(result.isSuccessful());
    }

    private VirtualMachineCurrentState createGpuAgentCurrentState(final String hostname, final String clusterAttrVal) {
        final long now = System.currentTimeMillis();
        final Map<String, Protos.Attribute> attributeMap = new HostMap<>();
        attributeMap.put(agentClusterAttributeName, Protos.Attribute.newBuilder().setName(agentClusterAttributeName)
                .setType(Protos.Value.Type.TEXT)
                .setText(Protos.Value.Text.newBuilder().setValue(clusterAttrVal)).build());
        attributeMap.put(
                "itype",
                Protos.Attribute.newBuilder().setName("itype")
                        .setType(Protos.Value.Type.TEXT)
                        .setText(Protos.Value.Text.newBuilder().setValue(AwsInstanceType.G2_2XLARGE_ID))
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