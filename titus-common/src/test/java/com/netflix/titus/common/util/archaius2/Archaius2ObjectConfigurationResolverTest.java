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

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.config.DefaultSettableConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Archaius2ObjectConfigurationResolverTest {

    private final DefaultSettableConfig configuration = new DefaultSettableConfig();

    private final ObjectConfigurationResolver<MyObject, MyObjectConfig> resolver = new Archaius2ObjectConfigurationResolver<>(
            configuration.getPrefixedView("object"),
            MyObject::getName,
            MyObjectConfig.class,
            Archaius2Ext.newConfiguration(MyObjectConfig.class, "default", configuration)
    );

    @Test
    public void testDefaultObjectConfiguration() {
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(100);
    }

    @Test
    public void testDefaultOverrides() {
        configuration.setProperty("default.limit", "200");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);
    }

    @Test
    public void testObjectOverrides() {
        configuration.setProperty("object.a.pattern", "a");
        configuration.setProperty("object.a.limit", "200");
        configuration.setProperty("object.b.pattern", "b");
        configuration.setProperty("object.b.limit", "300");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);
        assertThat(resolver.resolve(new MyObject("b")).getLimit()).isEqualTo(300);
        assertThat(resolver.resolve(new MyObject("x")).getLimit()).isEqualTo(100);
    }

    @Test
    public void testDynamicUpdates() {
        configuration.setProperty("object.a.pattern", "a");
        configuration.setProperty("object.a.limit", "200");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);

        configuration.setProperty("object.a.limit", "300");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(300);

        configuration.clearProperty("object.a.pattern");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(100);
    }

    @Test
    public void testBadPattern() {
        configuration.setProperty("object.a.pattern", "a");
        configuration.setProperty("object.a.limit", "200");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);

        configuration.setProperty("object.a.pattern", "*"); // bad regexp
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);
    }

    private static class MyObject {

        private final String name;

        MyObject(String name) {
            this.name = name;
        }

        String getName() {
            return name;
        }
    }

    private interface MyObjectConfig {
        @DefaultValue("100")
        int getLimit();
    }
}