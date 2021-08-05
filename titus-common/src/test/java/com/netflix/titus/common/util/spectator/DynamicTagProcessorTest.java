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

package com.netflix.titus.common.util.spectator;

import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Meter;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamicTagProcessorTest {

    private final DefaultRegistry registry = new DefaultRegistry();

    @Test
    public void testProcessor() {
        DynamicTagProcessor<Counter> processor = new DynamicTagProcessor<>(
                registry.createId("myRoot"),
                new String[]{"tag1", "tag2"},
                registry::counter
        );
        processor.withTags("v1_1", "v2_1").ifPresent(Counter::increment);
        processor.withTags("v1_2", "v2_1").ifPresent(counter -> counter.increment(5));
        processor.withTags("v1_2", "v2_2").ifPresent(counter -> counter.increment(10));
        // And again the first one
        processor.withTags("v1_1", "v2_1").ifPresent(Counter::increment);

        assertThat(getCounter("myRoot", "tag1", "v1_1", "tag2", "v2_1").count()).isEqualTo(2);
        assertThat(getCounter("myRoot", "tag1", "v1_2", "tag2", "v2_1").count()).isEqualTo(5);
        assertThat(getCounter("myRoot", "tag1", "v1_2", "tag2", "v2_2").count()).isEqualTo(10);
    }

    private Counter getCounter(String name, String... tags) {
        Id id = registry.createId(name, tags);
        Meter meter = registry.get(id);
        assertThat(meter).isNotNull().isInstanceOf(Counter.class);
        return (Counter) meter;
    }
}