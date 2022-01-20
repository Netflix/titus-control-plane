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

import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.runtime.RelocationAttributes;
import com.netflix.titus.runtime.connector.kubernetes.std.StdKubeApiFacade;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.TestDataFactory;
import io.kubernetes.client.openapi.models.V1Node;
import org.junit.Test;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.TAINT_EFFECT_NO_EXECUTE;
import static com.netflix.titus.supplementary.relocation.TestDataFactory.addNodeCondition;
import static com.netflix.titus.supplementary.relocation.TestDataFactory.addNodeTaint;
import static com.netflix.titus.supplementary.relocation.TestDataFactory.newNode;
import static org.assertj.core.api.Assertions.assertThat;

public class KubernetesNodeDataResolverTest {

    private final RelocationConfiguration configuration = Archaius2Ext.newConfiguration(RelocationConfiguration.class,
            "titus.relocation.nodeRelocationRequiredTaints", "required.*",
            "titus.relocation.nodeRelocationRequiredImmediatelyTaints", "immediately.*",
            "titus.relocation.badNodeConditionPattern", ".*MemoryFailure"
    );

    @Test
    public void testResolver() {
        String node1Name = "node1";
        String node2Name = "node2";
        V1Node node1 = newNode(node1Name);
        V1Node node2 = newNode(node2Name);

        StdKubeApiFacade kubeApiFacade = TestDataFactory.mockKubeApiFacade(node1, node2);
        KubernetesNodeDataResolver resolver = new KubernetesNodeDataResolver(configuration, kubeApiFacade, node -> true);
        Map<String, Node> resolved = resolver.resolve();
        assertThat(resolved).hasSize(2);

        // Nothing is flagged yet
        assertThat(resolver.resolve().get(node1Name).isRelocationRequired()).isFalse();
        assertThat(resolver.resolve().get(node1Name).isRelocationRequiredImmediately()).isFalse();
        assertThat(resolver.resolve().get(node1Name).isRelocationNotAllowed()).isFalse();
        assertThat(resolver.resolve().get(node1Name).isInBadCondition()).isFalse();

        // Tag one as removable
        addNodeTaint(node1, "required.titus.com/decommissioning", "true", TAINT_EFFECT_NO_EXECUTE);
        node1.getMetadata().getLabels().put(RelocationAttributes.RELOCATION_REQUIRED, "true");
        assertThat(resolver.resolve().get(node1Name).isRelocationRequired()).isTrue();
        assertThat(resolver.resolve().get(node1Name).isRelocationRequiredImmediately()).isFalse();
        assertThat(resolver.resolve().get(node1Name).isRelocationNotAllowed()).isFalse();

        // Now removable immediately
        addNodeTaint(node1, "immediately.titus.com/decommissioning", "true", TAINT_EFFECT_NO_EXECUTE);
        assertThat(resolver.resolve().get(node1Name).isRelocationRequiredImmediately()).isTrue();
        assertThat(resolver.resolve().get(node1Name).isRelocationNotAllowed()).isFalse();

        // bad memory condition = True
        addNodeCondition(node2, "CorrectableMemoryFailure", "True");
        assertThat(resolver.resolve().get(node2Name)).isNotNull();
        assertThat(resolver.resolve().get(node2Name).isInBadCondition()).isTrue();

        // bad memory condition = False
        addNodeCondition(node1, "CorrectableMemoryFailure", "False");
        assertThat(resolver.resolve().get(node1Name)).isNotNull();
        assertThat(resolver.resolve().get(node1Name).isInBadCondition()).isFalse();
    }
}