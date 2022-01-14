/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.kubernetes;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1ContainerState;
import io.kubernetes.client.openapi.models.V1ContainerStateRunning;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodStatus;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class KubeUtilTest {

    private static final String FARZONE_A = "farzoneA";
    private static final String FARZONE_B = "farzoneB";
    private static final String NOT_FARZONE = "notFarzone";
    private static final List<String> FARZONES = asList(FARZONE_A, FARZONE_B);

    private static final V1Node NODE_WITHOUT_ZONE = new V1Node().metadata(new V1ObjectMeta().labels(Collections.emptyMap()));

    @Test
    public void testIsFarzone() {
        assertThat(KubeUtil.isFarzoneNode(FARZONES, newNodeInZone(FARZONE_A))).isTrue();
        assertThat(KubeUtil.isFarzoneNode(asList(FARZONE_A, "farzoneB"), newNodeInZone(NOT_FARZONE))).isFalse();
        assertThat(KubeUtil.isFarzoneNode(asList(FARZONE_A, "farzoneB"), NODE_WITHOUT_ZONE)).isFalse();
    }

    @Test
    public void testEstimatePodSize() {
        assertThat(KubeUtil.estimatePodSize(new V1Pod())).isGreaterThan(0);
    }

    @Test
    public void testFindFinishedTimestamp() {
        // Test running pod
        V1Pod pod = new V1Pod().status(new V1PodStatus().containerStatuses(new ArrayList<>()));
        pod.getStatus().getContainerStatuses().add(new V1ContainerStatus()
                .state(new V1ContainerState().running(new V1ContainerStateRunning()))
        );
        assertThat(KubeUtil.findFinishedTimestamp(pod)).isEmpty();

        // Test finished pod
        pod.getStatus().getContainerStatuses().add(new V1ContainerStatus()
                .state(new V1ContainerState().terminated(new V1ContainerStateTerminated()))
        );
        assertThat(KubeUtil.findFinishedTimestamp(pod)).isEmpty();

        OffsetDateTime now = OffsetDateTime.now();
        pod.getStatus().getContainerStatuses().get(1).getState().getTerminated().finishedAt(now);
        assertThat(KubeUtil.findFinishedTimestamp(pod)).contains(now.toInstant().toEpochMilli());
    }

    private V1Node newNodeInZone(String zoneId) {
        return new V1Node()
                .metadata(new V1ObjectMeta().labels(Collections.singletonMap(KubeConstants.NODE_LABEL_ZONE, zoneId)))
                .spec(new V1NodeSpec().taints(new ArrayList<>()));
    }
}