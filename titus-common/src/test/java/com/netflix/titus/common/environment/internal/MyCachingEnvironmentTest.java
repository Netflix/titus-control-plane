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

package com.netflix.titus.common.environment.internal;

import java.util.Collections;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.environment.MyEnvironments;
import com.netflix.titus.common.environment.MyMutableEnvironment;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class MyCachingEnvironmentTest {

    private final DefaultRegistry registry = new DefaultRegistry();

    private final MyMutableEnvironment source = MyEnvironments.newMutableFromMap(Collections.emptyMap());

    private final MyCachingEnvironment cache = new MyCachingEnvironment(
            "junit",
            source,
            1,
            registry
    );

    @Test
    public void testRefresh() {
        source.setProperty("key1", "value1");
        assertThat(cache.getProperty("key1")).contains("value1");
        source.setProperty("key1", "value2");
        await().until(() -> cache.getProperty("key1").contains("value2"));
    }

    @Test
    public void testMissingProperty() {
        assertThat(cache.getProperty("key1")).isNull();
        source.setProperty("key1", "value1");
        await().until(() -> cache.getProperty("key1").contains("value1"));
    }

    @Test
    public void testRemovedProperty() {
        source.setProperty("key1", "value1");
        assertThat(cache.getProperty("key1")).contains("value1");
        source.removeProperty("key1");
        await().until(() -> cache.getProperty("key1") == null);
    }
}
