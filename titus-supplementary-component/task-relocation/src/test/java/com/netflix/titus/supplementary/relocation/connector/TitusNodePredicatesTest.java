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

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Pattern;

import com.netflix.titus.runtime.connector.kubernetes.fabric8io.Fabric8IOUtil;
import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeCondition;
import io.fabric8.kubernetes.api.model.NodeSpec;
import io.fabric8.kubernetes.api.model.NodeStatus;
import io.fabric8.kubernetes.api.model.Taint;
import io.fabric8.kubernetes.api.model.TaintBuilder;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TitusNodePredicatesTest {

    @Test
    public void testIsOwnedByScheduler() {
        Node node = new NodeBuilder(false)
                .editOrNewSpec()
                .addToTaints(new TaintBuilder()
                        .withKey(KubeConstants.TAINT_SCHEDULER)
                        .withValue("fenzo")
                        .build()
                )
                .endSpec()
                .build();
        assertThat(NodePredicates.isOwnedByScheduler("fenzo", node)).isTrue();
        assertThat(NodePredicates.isOwnedByScheduler("kubeScheduler", node)).isFalse();
    }

    @Test
    public void nodeConditionTransitionThreshold() {
        NodeCondition nodeCondition1 = new NodeCondition();
        nodeCondition1.setLastTransitionTime(Fabric8IOUtil.formatTimestamp(OffsetDateTime.now().minusMinutes(10)));
        boolean isTransitionRecent = NodePredicates.isNodeConditionTransitionedRecently(nodeCondition1, 300);
        assertThat(isTransitionRecent).isFalse();

        NodeCondition nodeCondition2 = new NodeCondition();
        nodeCondition2.setLastTransitionTime(Fabric8IOUtil.formatTimestamp(OffsetDateTime.now().minusSeconds(100)));
        boolean isTransitionRecent2 = NodePredicates.isNodeConditionTransitionedRecently(nodeCondition2, 300);
        assertThat(isTransitionRecent2).isTrue();
    }

    @Test
    public void checkBadNodeCondition() {
        Node node = new Node();

        NodeCondition condition1 = new NodeCondition();
        condition1.setLastTransitionTime(Fabric8IOUtil.formatTimestamp(OffsetDateTime.now().minusMinutes(10)));
        condition1.setType("CorruptedMemoryFailure");
        condition1.setMessage("There isn't that much corrupt memory");
        condition1.setReason("CorruptedMemoryIsUnderThreshold");
        condition1.setStatus("true");

        NodeCondition condition2 = new NodeCondition();
        condition2.setLastTransitionTime(Fabric8IOUtil.formatTimestamp(OffsetDateTime.now().minusMinutes(10)));
        condition2.setType("EniCarrierProblem");
        condition2.setMessage("Enis are working");
        condition2.setReason("EnisAreWorking");
        condition2.setStatus("False");

        NodeStatus v1NodeStatus = new NodeStatus();
        v1NodeStatus.setConditions(Arrays.asList(condition1, condition2));
        node.setStatus(v1NodeStatus);

        Pattern pattern = Pattern.compile(".*MemoryFailure");
        boolean isBadCondition = NodePredicates.hasBadCondition(node, pattern::matcher, 300);
        assertThat(isBadCondition).isTrue();

        condition1.setStatus("False");
        isBadCondition = NodePredicates.hasBadCondition(node, pattern::matcher, 300);
        assertThat(isBadCondition).isFalse();
    }

    @Test
    public void checkBadTaint() {
        Node node = new Node();
        NodeSpec nodeSpec = new NodeSpec();

        Taint taint1 = new Taint();
        taint1.setEffect("NoSchedule");
        taint1.setKey("node.titus.netflix.com/tier");
        taint1.setValue("Critical");
        taint1.setTimeAdded(Fabric8IOUtil.formatTimestamp(OffsetDateTime.now().minusMinutes(20)));

        Taint taint2 = new Taint();
        taint2.setEffect("NoSchedule");
        taint2.setKey("node.kubernetes.io/unreachable");
        taint2.setTimeAdded(Fabric8IOUtil.formatTimestamp(OffsetDateTime.now().minusMinutes(10)));

        nodeSpec.setTaints(Arrays.asList(taint1, taint2));
        node.setSpec(nodeSpec);

        Pattern pattern = Pattern.compile(".*unreachable");
        boolean isBadTaint = NodePredicates.hasBadTaint(node, pattern::matcher, 300);
        assertThat(isBadTaint).isTrue();

        nodeSpec.setTaints(Collections.singletonList(taint1));
        isBadTaint = NodePredicates.hasBadTaint(node, pattern::matcher, 300);
        assertThat(isBadTaint).isFalse();
    }
}