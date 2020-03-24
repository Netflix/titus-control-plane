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

package com.netflix.titus.common.util.spectator;

import java.util.Collections;
import java.util.List;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class MultiDimensionalGaugeTest {

    private final Registry registry = new DefaultRegistry();

    private final Id rootId = registry.createId("junit");

    @Test
    public void testUpdates() {
        MultiDimensionalGauge mg = SpectatorExt.multiDimensionalGauge(rootId, asList("tagA", "tagB"), registry);

        // Initial state
        mg.beginUpdate()
                .set(asList("tagA", "a1", "tagB", "b1"), 1)
                .set(asList("tagB", "b2", "tagA", "a2"), 2)
                .commit();

        assertGauge(mg, asList("a1", "b1"), 1);
        assertGauge(mg, asList("a2", "b2"), 2);

        // Update
        mg.beginUpdate()
                .set(asList("tagB", "b1", "tagA", "a1"), 3)
                .set(asList("tagB", "b3", "tagA", "a1"), 4)
                .commit();

        assertGauge(mg, asList("a1", "b1"), 3);
        assertGauge(mg, asList("a1", "b3"), 4);
        assertGaugeMissing(mg, asList("a2", "b2"));

        // Clear
        mg.remove();
        assertThat(mg.gaugesByTagValues).isEmpty();
    }

    @Test
    public void testRevisionOrder() {
        MultiDimensionalGauge mg = SpectatorExt.multiDimensionalGauge(rootId, Collections.singletonList("tagA"), registry);

        MultiDimensionalGauge.Setter setter1 = mg.beginUpdate()
                .set(asList("tagA", "a1"), 1);
        mg.beginUpdate()
                .set(asList("tagA", "a1"), 2)
                .commit();
        setter1.commit();

        assertGauge(mg, Collections.singletonList("a1"), 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailsOnBadTagName() {
        SpectatorExt.multiDimensionalGauge(rootId, Collections.singletonList("tagA"), registry)
                .beginUpdate()
                .set(asList("tagB", "a1"), 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFailsOnBadTagCount() {
        SpectatorExt.multiDimensionalGauge(rootId, asList("tagA", "tagB"), registry)
                .beginUpdate()
                .set(asList("tagA", "a1"), 1)
                .commit();
    }

    private void assertGauge(MultiDimensionalGauge mg, List<String> tagValues, double expectedValue) {
        Gauge gauge = mg.gaugesByTagValues.get(tagValues);
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(expectedValue);
    }

    private void assertGaugeMissing(MultiDimensionalGauge mg, List<String> tagValues) {
        Gauge gauge = mg.gaugesByTagValues.get(tagValues);
        assertThat(gauge).isNull();
    }
}