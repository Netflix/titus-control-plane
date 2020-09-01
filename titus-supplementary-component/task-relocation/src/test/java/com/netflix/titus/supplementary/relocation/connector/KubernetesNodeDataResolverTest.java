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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.runtime.connector.kubernetes.KubeApiFacade;
import com.netflix.titus.runtime.RelocationAttributes;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import io.kubernetes.client.informer.SharedIndexInformer;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Taint;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.runtime.kubernetes.KubeConstants.NODE_LABEL_MACHINE_GROUP;
import static com.netflix.titus.runtime.kubernetes.KubeConstants.TAINT_EFFECT_NO_EXECUTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KubernetesNodeDataResolverTest {

    private final RelocationConfiguration configuration = Archaius2Ext.newConfiguration(RelocationConfiguration.class,
            "titus.relocation.nodeRelocationRequiredTaints", "required.*",
            "titus.relocation.nodeRelocationRequiredImmediatelyTaints", "immediately.*"
    );

    private final KubeApiFacade kubeApiFacade = mock(KubeApiFacade.class);

    private final SharedIndexInformer<V1Node> nodeInformer = mock(SharedIndexInformer.class);

    private final Indexer<V1Node> nodeIndexer = mock(Indexer.class);

    private final List<V1Node> nodeIndex = new ArrayList<>();

    private KubernetesNodeDataResolver resolver;

    @Before
    public void setUp() {
        when(kubeApiFacade.getNodeInformer()).thenReturn(nodeInformer);
        when(nodeInformer.getIndexer()).thenReturn(nodeIndexer);
        when(nodeIndexer.list()).thenAnswer(invocation -> nodeIndex);

        resolver = new KubernetesNodeDataResolver(configuration, kubeApiFacade, node -> true);
    }

    @Test
    public void testResolver() {
        String node1Name = "node1";
        V1Node node1 = newNode(node1Name);
        nodeIndex.add(node1);
        nodeIndex.add(newNode("node2"));
        Map<String, Node> resolved = resolver.resolve();
        assertThat(resolved).hasSize(2);

        // Nothing is flagged yet
        assertThat(resolver.resolve().get(node1Name).isRelocationRequired()).isFalse();
        assertThat(resolver.resolve().get(node1Name).isRelocationRequiredImmediately()).isFalse();
        assertThat(resolver.resolve().get(node1Name).isRelocationNotAllowed()).isFalse();

        // Tag one as removable
        addNoExecuteTaint(node1, "required.titus.com/decommissioning");
        node1.getMetadata().getLabels().put(RelocationAttributes.RELOCATION_REQUIRED, "true");
        assertThat(resolver.resolve().get(node1Name).isRelocationRequired()).isTrue();
        assertThat(resolver.resolve().get(node1Name).isRelocationRequiredImmediately()).isFalse();
        assertThat(resolver.resolve().get(node1Name).isRelocationNotAllowed()).isFalse();

        // Now removable immediately
        addNoExecuteTaint(node1, "immediately.titus.com/decommissioning");
        assertThat(resolver.resolve().get(node1Name).isRelocationRequiredImmediately()).isTrue();
        assertThat(resolver.resolve().get(node1Name).isRelocationNotAllowed()).isFalse();
    }

    private V1Node newNode(String id) {
        return new V1Node()
                .metadata(new V1ObjectMeta()
                        .name(id)
                        .labels(CollectionsExt.asMap(
                                NODE_LABEL_MACHINE_GROUP, "serverGroup1"
                        ))
                )
                .spec(new V1NodeSpec()
                        .taints(new ArrayList<>())
                );
    }

    private void addNoExecuteTaint(V1Node node, String taintName) {
        node.getSpec().getTaints().add(new V1Taint()
                .key(taintName)
                .effect(TAINT_EFFECT_NO_EXECUTE)
        );
    }
}