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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Taint;
import org.junit.Test;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class KubeUtilTest {

    private static final String FARZONE_A = "farzoneA";
    private static final String FARZONE_B = "farzoneB";
    private static final String NOT_FARZONE = "notFarzone";
    private static final List<String> FARZONES = asList(FARZONE_A, FARZONE_B);

    private static final V1Node NODE_WITHOUT_ZONE = new V1Node().metadata(new V1ObjectMeta().labels(Collections.emptyMap()));

    private static final V1Taint TAINT_SCHEDULER_FENZO = new V1Taint().key(KubeConstants.TAINT_SCHEDULER).value(KubeConstants.TAINT_SCHEDULER_VALUE_FENZO);
    private static final V1Taint TAINT_SCHEDULER_OTHER = new V1Taint().key(KubeConstants.TAINT_SCHEDULER).value(KubeConstants.TAINT_SCHEDULER_VALUE_KUBE);
    private static final V1Taint TAINT_TOLERATED_TAINT_1 = new V1Taint().key("toleratedTaint1").value("someValue");
    private static final V1Taint TAINT_TOLERATED_TAINT_2 = new V1Taint().key("toleratedTaint2").value("someValue");
    private static final V1Taint TAINT_NOT_TOLERATED_TAINT = new V1Taint().key("notToleratedTaint").value("someValue");

    private static final Set<String> TOLERATED_TAINTS = asSet(TAINT_TOLERATED_TAINT_1.getKey(), TAINT_TOLERATED_TAINT_2.getKey());

    @Test
    public void testIsFarzone() {
        assertThat(KubeUtil.isFarzoneNode(FARZONES, newNodeInZone(FARZONE_A))).isTrue();
        assertThat(KubeUtil.isFarzoneNode(asList(FARZONE_A, "farzoneB"), newNodeInZone(NOT_FARZONE))).isFalse();
        assertThat(KubeUtil.isFarzoneNode(asList(FARZONE_A, "farzoneB"), NODE_WITHOUT_ZONE)).isFalse();
    }

    @Test
    public void testHasFenzoSchedulerTaint() {
        assertThat(KubeUtil.hasFenzoSchedulerTaint(newNodeWithoutZone())).isTrue();
        assertThat(KubeUtil.hasFenzoSchedulerTaint(newNodeWithoutZone(TAINT_NOT_TOLERATED_TAINT))).isTrue();
        assertThat(KubeUtil.hasFenzoSchedulerTaint(newNodeWithoutZone(TAINT_SCHEDULER_FENZO))).isTrue();
        assertThat(KubeUtil.hasFenzoSchedulerTaint(newNodeWithoutZone(TAINT_NOT_TOLERATED_TAINT, TAINT_SCHEDULER_FENZO))).isTrue();

        assertThat(KubeUtil.hasFenzoSchedulerTaint(newNodeWithoutZone(TAINT_SCHEDULER_OTHER))).isFalse();
        assertThat(KubeUtil.hasFenzoSchedulerTaint(newNodeWithoutZone(TAINT_SCHEDULER_OTHER, TAINT_SCHEDULER_FENZO))).isFalse();
    }

    @Test
    public void testIsNodeOwnedByFenzo() {
        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeInZone(FARZONE_A))).isFalse();
        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeInZone(NOT_FARZONE))).isTrue();

        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeWithoutZone())).isTrue();

        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeWithoutZone(TAINT_SCHEDULER_OTHER))).isFalse();
        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeWithoutZone(TAINT_SCHEDULER_OTHER, TAINT_TOLERATED_TAINT_1, TAINT_NOT_TOLERATED_TAINT))).isFalse();
        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeWithoutZone(TAINT_SCHEDULER_OTHER, TAINT_TOLERATED_TAINT_1, TAINT_TOLERATED_TAINT_2))).isFalse();

        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeWithoutZone(TAINT_SCHEDULER_FENZO))).isTrue();
        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeWithoutZone(TAINT_SCHEDULER_FENZO, TAINT_TOLERATED_TAINT_1, TAINT_NOT_TOLERATED_TAINT))).isFalse();
        assertThat(KubeUtil.isNodeOwnedByFenzo(FARZONES, TOLERATED_TAINTS, newNodeWithoutZone(TAINT_SCHEDULER_FENZO, TAINT_TOLERATED_TAINT_1, TAINT_TOLERATED_TAINT_2))).isTrue();
    }

    private V1Node newNodeWithoutZone(V1Taint... taints) {
        V1Node node = new V1Node()
                .metadata(new V1ObjectMeta().labels(Collections.emptyMap()))
                .spec(new V1NodeSpec().taints(new ArrayList<>()));
        for (V1Taint taint : taints) {
            node.getSpec().getTaints().add(taint);
        }
        return node;
    }

    private V1Node newNodeInZone(String zoneId) {
        return new V1Node()
                .metadata(new V1ObjectMeta().labels(Collections.singletonMap(KubeConstants.NODE_LABEL_ZONE, zoneId)))
                .spec(new V1NodeSpec().taints(new ArrayList<>()));
    }
}