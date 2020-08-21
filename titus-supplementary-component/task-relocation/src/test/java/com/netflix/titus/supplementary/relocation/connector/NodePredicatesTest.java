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

import com.netflix.titus.runtime.kubernetes.KubeConstants;
import io.kubernetes.client.openapi.models.V1Node;
import io.kubernetes.client.openapi.models.V1NodeSpec;
import io.kubernetes.client.openapi.models.V1Taint;
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
}