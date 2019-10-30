/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.util.archaius2;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;

import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.archaius.config.MapConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Archaius2ExtTest {

    @Test
    public void testAsDurationList() {
        assertThat(Archaius2Ext.asDurationList(() -> "1, 2, 3").get()).contains(
                Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(3)
        );
    }

    @Test
    public void testPropertyUpdate() {
        SettableConfig config = new DefaultSettableConfig();
        config.setProperty("a", 1);

        DefaultPropertyFactory factory = new DefaultPropertyFactory(config);
        Iterator<Integer> it = Archaius2Ext.watch(factory, "a", Integer.class).toIterable().iterator();
        assertThat(it.next()).isEqualTo(1);

        config.setProperty("a", 2);
        factory.invalidate();
        assertThat(it.next()).isEqualTo(2);
    }

    @Test
    public void testConfiguration() {
        assertThat(Archaius2Ext.newConfiguration(MyConfig.class).getString()).isEqualTo("hello");
    }

    @Test
    public void testConfigurationWithOverrides() {
        assertThat(Archaius2Ext.newConfiguration(MyConfig.class, "string", "overridden").getString()).isEqualTo("overridden");
    }

    @Test
    public void testConfigurationWithNoPrefix() {
        MapConfig config = new MapConfig(Collections.singletonMap("string", "HELLO"));
        assertThat(Archaius2Ext.newConfiguration(MyConfig.class, config).getString()).isEqualTo("HELLO");
    }

    @Test
    public void testConfigurationWithPrefix() {
        MapConfig config = new MapConfig(Collections.singletonMap("root.string", "HELLO"));
        assertThat(Archaius2Ext.newConfiguration(MyConfig.class, "root", config).getString()).isEqualTo("HELLO");
    }

    private interface MyConfig {
        @DefaultValue("hello")
        String getString();
    }
}