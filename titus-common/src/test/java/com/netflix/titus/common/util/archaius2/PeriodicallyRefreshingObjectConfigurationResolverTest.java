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

package com.netflix.titus.common.util.archaius2;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.common.environment.MyEnvironment;
import com.netflix.titus.common.environment.MyEnvironments;
import com.netflix.titus.common.util.closeable.CloseableReference;
import org.junit.After;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;
import reactor.core.publisher.DirectProcessor;

import static org.assertj.core.api.Assertions.assertThat;

public class PeriodicallyRefreshingObjectConfigurationResolverTest {

    private final MockEnvironment configuration = new MockEnvironment();

    private final DirectProcessor<Long> updateTrigger = DirectProcessor.create();

    private final CloseableReference<ObjectConfigurationResolver<MyObject, MyObjectConfig>> resolverRef = Archaius2Ext.newObjectConfigurationResolver(
            "object",
            configuration,
            MyObject::getName,
            MyObjectConfig.class,
            Archaius2Ext.newConfiguration(MyObjectConfig.class, "default", MyEnvironments.newSpring(configuration)),
            updateTrigger
    );

    private final ObjectConfigurationResolver<MyObject, MyObjectConfig> resolver = resolverRef.get();

    @After
    public void tearDown() {
        resolverRef.close();
    }

    @Test
    public void testDefaultObjectConfiguration() {
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(100);
    }

    @Test
    public void testDefaultOverrides() {
        update("default.limit", "200");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);
    }

    @Test
    public void testObjectOverrides() {
        update("object.a.pattern", "a",
                "object.a.limit", "200",
                "object.b.pattern", "b",
                "object.b.limit", "300"
        );
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);
        assertThat(resolver.resolve(new MyObject("b")).getLimit()).isEqualTo(300);
        assertThat(resolver.resolve(new MyObject("x")).getLimit()).isEqualTo(100);
    }

    @Test
    public void testDynamicUpdates() {
        update("object.a.pattern", "a",
                "object.a.limit", "200"
        );
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);

        update("object.a.limit", "300");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(300);

        update("object.a.pattern", "not_a_anymore");
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(100);
    }

    @Test
    public void testBadPattern() {
        update("object.a.pattern", "a",
                "object.a.limit", "200"
        );
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);

        update("object.a.pattern", "*"); // bad regexp
        assertThat(resolver.resolve(new MyObject("a")).getLimit()).isEqualTo(200);
    }

    private void update(String... keyValuePairs) {
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            configuration.setProperty(keyValuePairs[i], keyValuePairs[i + 1]);
        }
        updateTrigger.onNext(System.currentTimeMillis());
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
