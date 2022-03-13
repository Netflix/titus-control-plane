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

package com.netflix.titus.common.util.event;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EventPropagationUtilTest {

    private static final List<String> CLIENT_LABELS = Arrays.asList("tjcInternal", "gatewayClient", "gatewayInternal", "federationClient", "federationInternal", "client");
    private static final List<String> FEDERATION_LABELS = Arrays.asList("tjcInternal", "gatewayClient", "gatewayInternal", "federationClient", "federationInternal");

    @Test
    public void testTitusPropagationTrackingInClient() {
        Map<String, String> attributes = Collections.singletonMap("keyA", "valueA");
        // TJC -> Gateway
        Map<String, String> stage1 = EventPropagationUtil.copyAndAddNextStage("s1", attributes, 100, 150);
        assertThat(stage1).containsEntry(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES, "s1(50ms):before=100,after=150");
        // Gateway -> Federation
        Map<String, String> stage2 = EventPropagationUtil.copyAndAddNextStage("s2", stage1, 300, 400);
        assertThat(stage2).containsEntry(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES, "s1(50ms):before=100,after=150;s2(100ms):before=300,after=400");
        // Federation -> client
        Map<String, String> stage3 = EventPropagationUtil.copyAndAddNextStage("s3", stage2, 1000, 1200);
        assertThat(stage3).containsEntry(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES, "s1(50ms):before=100,after=150;s2(100ms):before=300,after=400;s3(200ms):before=1000,after=1200");

        // Result
        EventPropagationTrace clientTrace = EventPropagationUtil.parseTrace(stage3, false, 10, CLIENT_LABELS).orElse(null);
        assertThat(clientTrace).isNotNull();
        assertThat(clientTrace.getStages()).containsEntry("tjcInternal", 90L);
        assertThat(clientTrace.getStages()).containsEntry("gatewayClient", 50L);
        assertThat(clientTrace.getStages()).containsEntry("gatewayInternal", 150L);
        assertThat(clientTrace.getStages()).containsEntry("federationClient", 100L);
        assertThat(clientTrace.getStages()).containsEntry("federationInternal", 600L);
        assertThat(clientTrace.getStages()).containsEntry("client", 200L);
    }

    @Test
    public void testTitusPropagationTrackingInFederation() {
        Map<String, String> attributes = Collections.singletonMap(
                EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES, "before=100,after=150;before=300,after=400;before=1000,after=1000"
        );

        // Federation trace does not include federation -> client latency, but includes federation internal processing
        EventPropagationTrace federationTrace = EventPropagationUtil.parseTrace(attributes, false, 10, FEDERATION_LABELS).orElse(null);
        assertThat(federationTrace).isNotNull();
        assertThat(federationTrace.getStages()).containsEntry("federationInternal", 600L);
        assertThat(federationTrace.getStages()).doesNotContainKey("client");
    }

    @Test
    public void testTitusPropagationTrackingTooLong () {
        Map<String, String> attributes = Collections.singletonMap("keyA", "valueA");
        int ts = 0;
        for (int i = 0; i < 100; i++) {
            ts = ts + 100;
            attributes = EventPropagationUtil.copyAndAddNextStage("s" + i, attributes, ts, ts + 200);
            assertThat(attributes).containsKey(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES);
        }

        EventPropagationTrace clientTrace = EventPropagationUtil.parseTrace(attributes, false, ts, CLIENT_LABELS).orElse(null);
        assertThat(clientTrace).isNotNull();
        assertThat(attributes).containsKey(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES);
        String traceStr = attributes.get(EventPropagationUtil.EVENT_ATTRIBUTE_PROPAGATION_STAGES);
        assertThat(traceStr.length()).isLessThan(EventPropagationUtil.MAX_LENGTH_EVENT_PROPAGATION_STAGE);
    }

}