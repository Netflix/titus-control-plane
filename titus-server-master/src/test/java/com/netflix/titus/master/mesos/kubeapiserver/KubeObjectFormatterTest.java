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

package com.netflix.titus.master.mesos.kubeapiserver;

import com.netflix.titus.master.kubernetes.KubeObjectFormatter;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1Pod;
import org.junit.Test;

import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.andIpAddress;
import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.andNodeAllocatableResources;
import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.andNodeLabels;
import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.andNodePhase;
import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.andTaint;
import static com.netflix.titus.master.mesos.kubeapiserver.NodeDataGenerator.newNode;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andLabel;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andNodeName;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andPhase;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.andReason;
import static com.netflix.titus.master.mesos.kubeapiserver.PodDataGenerator.newPod;
import static org.assertj.core.api.Assertions.assertThat;

public class KubeObjectFormatterTest {

    @Test
    public void testFormatPodEssentials() {
        V1Pod pod = newPod("testPod",
                andLabel("labelA", "valueA"),
                andPhase("RUNNING"), andReason("reason started"),
                andNodeName("someNode")
        );
        String result = KubeObjectFormatter.formatPodEssentials(pod);
        assertThat(result).isEqualTo("{name=testPod, labels={labelA=valueA}, nodeName=someNode, phase=RUNNING, reason=reason started}");
    }

    @Test
    public void testFormatNodeEssentials() {
        V1Node node = newNode("testNode",
                andNodeAllocatableResources(64, 8192, 16384, 512),
                andNodePhase("READY"),
                andNodeLabels("labelA", "valueA"),
                andIpAddress("nodeIpAddress"),
                andTaint("taintKey", "taintValue", "NoExecute")
        );
        String result = KubeObjectFormatter.formatNodeEssentials(node);
        assertThat(result).isEqualTo("{name=testNode, labels={labelA=valueA}, taints=[{key=taintKey, value=taintValue, effect=NoExecute], phase=READY, allocatableResources={disk=16384000000, memory=8192000000, cpu=64, network=536870912}}");
    }
}