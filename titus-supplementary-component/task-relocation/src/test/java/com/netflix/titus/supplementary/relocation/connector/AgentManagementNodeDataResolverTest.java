/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.supplementary.relocation.connector;

import java.util.Map;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.runtime.connector.agent.AgentDataReplicator;
import com.netflix.titus.supplementary.relocation.AbstractTaskRelocationTest;
import com.netflix.titus.supplementary.relocation.RelocationAttributes;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class AgentManagementNodeDataResolverTest extends AbstractTaskRelocationTest {

    private final AgentDataReplicator agentDataReplicator = mock(AgentDataReplicator.class);

    public AgentManagementNodeDataResolverTest() {
        super(TestDataFactory.activeRemovableSetup());
    }

    @Test
    public void testResolver() {
        AgentManagementNodeDataResolver resolver = new AgentManagementNodeDataResolver(agentOperations, agentDataReplicator, instance -> true);
        Map<String, Node> resolved = resolver.resolve();
        int expectedCount = agentOperations.findAgentInstances(pair -> true).stream().mapToInt(p -> p.getRight().size()).sum();
        assertThat(resolved).hasSize(expectedCount);

        // Nothing is flagged yet
        String instanceId = CollectionsExt.first(resolved.keySet());
        assertThat(resolver.resolve().get(instanceId).isRelocationRequired()).isFalse();
        assertThat(resolver.resolve().get(instanceId).isRelocationRequiredImmediately()).isFalse();
        assertThat(resolver.resolve().get(instanceId).isRelocationNotAllowed()).isFalse();

        // Tag one as removable
        relocationConnectorStubs.addInstanceAttribute(instanceId, RelocationAttributes.RELOCATION_REQUIRED, "true");
        assertThat(resolver.resolve().get(instanceId).isRelocationRequired()).isTrue();
        assertThat(resolver.resolve().get(instanceId).isRelocationRequiredImmediately()).isFalse();
        assertThat(resolver.resolve().get(instanceId).isRelocationNotAllowed()).isFalse();

        // Now removable immediately
        relocationConnectorStubs.addInstanceAttribute(instanceId, RelocationAttributes.RELOCATION_REQUIRED_IMMEDIATELY, "true");
        assertThat(resolver.resolve().get(instanceId).isRelocationRequiredImmediately()).isTrue();
        assertThat(resolver.resolve().get(instanceId).isRelocationNotAllowed()).isFalse();

        // Relocation not allowed
        relocationConnectorStubs.addInstanceAttribute(instanceId, RelocationAttributes.RELOCATION_NOT_ALLOWED, "true");
        assertThat(resolver.resolve().get(instanceId).isRelocationNotAllowed()).isTrue();
    }
}