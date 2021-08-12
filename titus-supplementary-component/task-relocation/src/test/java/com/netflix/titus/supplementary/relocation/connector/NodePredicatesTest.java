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

import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeCondition;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1NodeStatus;
import io.kubernetes.client.openapi.models.V1Taint;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NodePredicatesTest {

    @Test
    public void testIsOwnedByScheduler() {
        V1Taint taint = new V1Taint().key(KubeConstants.TAINT_SCHEDULER).value("fenzo");
        V1Node node = new V1Node().spec(new V1NodeSpec().taints(Collections.singletonList(taint)));
        assertThat(NodePredicates.isOwnedByScheduler("fenzo", node)).isTrue();
        assertThat(NodePredicates.isOwnedByScheduler("kubeScheduler", node)).isFalse();
    }

    @Test
    public void nodeConditionTransitionThreshold() {
        V1NodeCondition nodeCondition1 = new V1NodeCondition();
        nodeCondition1.setLastTransitionTime(OffsetDateTime.now().minusMinutes(10));
        boolean isTransitionRecent = NodePredicates.isNodeConditionTransitionedRecently(nodeCondition1, 300);
        assertThat(isTransitionRecent).isFalse();

        V1NodeCondition nodeCondition2 = new V1NodeCondition();
        nodeCondition2.setLastTransitionTime(OffsetDateTime.now().minusSeconds(100));
        boolean isTransitionRecent2 = NodePredicates.isNodeConditionTransitionedRecently(nodeCondition2, 300);
        assertThat(isTransitionRecent2).isTrue();
    }

    @Test
    public void checkBadNodeCondition() {
        V1Node v1Node = new V1Node();

        V1NodeCondition condition1 = new V1NodeCondition();
        condition1.setLastTransitionTime(OffsetDateTime.now().minusMinutes(10));
        condition1.setType("CorruptedMemoryFailure");
        condition1.setMessage("There isn't that much corrupt memory");
        condition1.setReason("CorruptedMemoryIsUnderThreshold");
        condition1.setStatus("true");

        V1NodeCondition condition2 = new V1NodeCondition();
        condition2.setLastTransitionTime(OffsetDateTime.now().minusMinutes(10));
        condition2.setType("EniCarrierProblem");
        condition2.setMessage("Enis are working");
        condition2.setReason("EnisAreWorking");
        condition2.setStatus("False");

        V1NodeStatus v1NodeStatus = new V1NodeStatus();
        v1NodeStatus.setConditions(Arrays.asList(condition1, condition2));
        v1Node.setStatus(v1NodeStatus);

        Pattern pattern = Pattern.compile(".*MemoryFailure");
        boolean isBadCondition = NodePredicates.hasBadCondition(v1Node, pattern::matcher, 300);
        assertThat(isBadCondition).isTrue();

        condition1.setStatus("False");
        isBadCondition = NodePredicates.hasBadCondition(v1Node, pattern::matcher, 300);
        assertThat(isBadCondition).isFalse();
    }

    @Test
    public void checkBadTaint() {
        V1Node v1Node = new V1Node();
        V1NodeSpec v1NodeSpec = new V1NodeSpec();

        V1Taint taint1 = new V1Taint();
        taint1.setEffect("NoSchedule");
        taint1.setKey("node.titus.netflix.com/tier");
        taint1.setValue("Critical");
        taint1.setTimeAdded(OffsetDateTime.now().minusMinutes(20));

        V1Taint taint2 = new V1Taint();
        taint2.setEffect("NoSchedule");
        taint2.setKey("node.kubernetes.io/unreachable");
        taint2.setTimeAdded(OffsetDateTime.now().minusMinutes(10));

        v1NodeSpec.setTaints(Arrays.asList(taint1, taint2));
        v1Node.setSpec(v1NodeSpec);

        Pattern pattern = Pattern.compile(".*unreachable");
        boolean isBadTaint = NodePredicates.hasBadTaint(v1Node, pattern::matcher, 300);
        assertThat(isBadTaint).isTrue();

        v1NodeSpec.setTaints(Collections.singletonList(taint1));
        isBadTaint = NodePredicates.hasBadTaint(v1Node, pattern::matcher, 300);
        assertThat(isBadTaint).isFalse();
    }
}