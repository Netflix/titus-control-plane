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

package com.netflix.titus.master.agent.service.resolver;

import java.util.Collections;

import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.scheduler.SchedulingService;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FenzoAgentClusterResolverTest {

    private final SchedulingService schedulingService = mock(SchedulingService.class);

    private final FenzoAgentClusterResolver resolver = new FenzoAgentClusterResolver(schedulingService);

    @Test
    public void testResolve() throws Exception {
        VirtualMachineCurrentState vmCurrentState = mock(VirtualMachineCurrentState.class);
        when(schedulingService.getVmCurrentStates()).thenReturn(Collections.singletonList(vmCurrentState));
        when(vmCurrentState.getHostname()).thenReturn("myhost");
        assertThat(resolver.resolve(), hasItems("myhost"));
    }
}