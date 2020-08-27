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

import java.util.Collections;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingNodeDataResolverTest {

    @Test
    public void testAggregation() {
        AggregatingNodeDataResolver resolver = new AggregatingNodeDataResolver(asList(
                newDelegate("node1", 1),
                newDelegate("node2", 2))
        );
        assertThat(resolver.resolve()).hasSize(2);
    }

    private NodeDataResolver newDelegate(String nodeId, long stalenessMs) {
        Node node = Node.newBuilder()
                .withId(nodeId)
                .withServerGroupId("myServerGroup")
                .build();

        NodeDataResolver resolver = mock(NodeDataResolver.class);
        when(resolver.resolve()).thenReturn(Collections.singletonMap(nodeId, node));
        when(resolver.getStalenessMs()).thenReturn(stalenessMs);

        return resolver;
    }
}