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

package com.netflix.titus.api.agent.model.sanitizer;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AgentSanitizerBuilderTest {

    private final EntitySanitizer sanitizer = new AgentSanitizerBuilder().build();

    @Test
    public void testAgentServerGroupSanitization() throws Exception {
        AgentInstanceGroup agentInstanceGroup = AgentGenerator.agentServerGroups().getValue();
        assertThat(sanitizer.sanitize(agentInstanceGroup)).isEmpty();
        assertThat(sanitizer.validate(agentInstanceGroup)).isEmpty();
    }

    @Test
    public void testBadAgentServerGroupSanitization() throws Exception {
        AgentInstanceGroup agentInstanceGroup = AgentGenerator.agentServerGroups().getValue().toBuilder()
                .withId("    myId    ")
                .withMin(10)
                .withDesired(5)
                .withMax(1)
                .build();
        assertThat(sanitizer.sanitize(agentInstanceGroup).get().getId()).isEqualTo("myId");
        assertThat(sanitizer.validate(agentInstanceGroup)).hasSize(2);
    }

    @Test
    public void testAgentInstanceSanitization() throws Exception {
        AgentInstance agentInstance = AgentGenerator.agentInstances().getValue();
        assertThat(sanitizer.sanitize(agentInstance)).isEmpty();
        assertThat(sanitizer.validate(agentInstance)).isEmpty();
    }

    @Test
    public void testBadAgentInstanceSanitization() throws Exception {
        AgentInstance agentInstance = AgentGenerator.agentInstances().getValue().toBuilder()
                .withIpAddress(" 10.0.0.1  ")
                .withHostname(null)
                .build();
        assertThat(sanitizer.sanitize(agentInstance).get().getIpAddress()).isEqualTo("10.0.0.1");
        assertThat(sanitizer.validate(agentInstance)).hasSize(1);
    }
}