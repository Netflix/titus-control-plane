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

import java.util.Collections;
import java.util.List;

import com.netflix.fenzo.functions.Func1;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.fenzo.queues.QueuableTask;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskToClusterMapperTest {

    private final TaskToClusterMapper mapper = new TaskToClusterMapper();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private Func1<QueuableTask, List<String>> func1;

    private QueuableTask t0noGpu = mockQueuableTask(mock(QueuableTask.class), 0, false);
    private QueuableTask t0gpu = mockQueuableTask(mock(QueuableTask.class), 0, true);
    private QueuableTask t1gpu = mockQueuableTask(mock(QueuableTask.class), 1, true);
    private QueuableTask t1noGpu = mockQueuableTask(mock(QueuableTask.class), 1, false);

    private AgentInstanceGroup instanceGroupCriticalNoGpu;
    private AgentInstanceGroup instanceGroupCriticalGpu;
    private AgentInstanceGroup instanceGroupFlexNoGpu;
    private AgentInstanceGroup instanceGroupFlexGpu;

    @Before
    public void setUp() throws Exception {
        List<AgentInstanceGroup> instanceGroups = AgentGenerator.agentServerGroups().toList(4);
        this.instanceGroupCriticalNoGpu = instanceGroups.get(0).toBuilder().withInstanceType("m4.4xlarge")
                .withTier(Tier.Critical)
                .withResourceDimension(instanceGroups.get(1).getResourceDimension().toBuilder().withGpu(0).build())
                .build();
        this.instanceGroupCriticalGpu = instanceGroups.get(1).toBuilder().withInstanceType("g2.8xlarge")
                .withTier(Tier.Critical)
                .withResourceDimension(instanceGroups.get(1).getResourceDimension().toBuilder().withGpu(4).build())
                .build();
        this.instanceGroupFlexNoGpu = instanceGroups.get(0).toBuilder().withInstanceType("r3.8xlarge")
                .withTier(Tier.Flex)
                .withResourceDimension(instanceGroups.get(1).getResourceDimension().toBuilder().withGpu(0).build())
                .build();
        this.instanceGroupFlexGpu = instanceGroups.get(1).toBuilder().withInstanceType("g2.8xlarge")
                .withTier(Tier.Flex)
                .withResourceDimension(instanceGroups.get(1).getResourceDimension().toBuilder().withGpu(4).build())
                .build();

        when(agentManagementService.getInstanceGroups()).thenReturn(asList(instanceGroupCriticalNoGpu, instanceGroupCriticalGpu, instanceGroupFlexNoGpu, instanceGroupFlexGpu));

        mapper.update(agentManagementService);
        func1 = mapper.getMapperFunc1();
    }

    @Test
    public void testTier0NoGpuTask() throws Exception {
        final List<String> list = func1.call(t0noGpu);
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(instanceGroupCriticalNoGpu.getId(), list.get(0));
    }

    @Test
    public void testTier0GpuTask() throws Exception {
        final List<String> list = func1.call(t0gpu);
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(instanceGroupCriticalGpu.getId(), list.get(0));
    }

    @Test
    public void testTier1NoGpuTask() throws Exception {
        final List<String> list = func1.call(t1noGpu);
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(instanceGroupFlexNoGpu.getId(), list.get(0));
    }

    @Test
    public void testTier1GpuTask() throws Exception {
        final List<String> list = func1.call(t1gpu);
        assertNotNull(list);
        assertEquals(1, list.size());
        assertEquals(instanceGroupFlexGpu.getId(), list.get(0));
    }

    private static QueuableTask mockQueuableTask(QueuableTask mock, int tier, boolean gpu) {
        when(mock.getQAttributes()).thenReturn(new QAttributes.QAttributesAdaptor(tier, "bucket-" + tier));
        when(mock.getScalarRequests()).thenReturn(Collections.singletonMap("gpu", gpu ? 1.0 : 0.0));
        return mock;
    }
}