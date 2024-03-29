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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;
import com.netflix.titus.common.environment.MyEnvironments;
import com.netflix.titus.common.environment.MyMutableEnvironment;
import com.netflix.titus.common.util.CollectionsExt;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class ArchaiusProxyInvocationHandlerTest {

    private final MyMutableEnvironment environment = MyEnvironments.newMutable();

    private SomeConfiguration configuration;

    @Before
    public void setUp() {
        environment.setProperty("annotationPrefix.intWithNoDefault", "11");
        this.configuration = Archaius2Ext.newConfiguration(SomeConfiguration.class, environment);
    }

    @Test
    public void testInt() {
        assertThat(configuration.getInt()).isEqualTo(1);
        environment.setProperty("annotationPrefix.int", "123");
        await().until(() -> configuration.getInt() == 123);

        assertThat(configuration.getIntWithNoDefault()).isEqualTo(11);
    }

    @Test
    public void testLong() {
        assertThat(configuration.getLong()).isEqualTo(2L);
        environment.setProperty("annotationPrefix.long", "123");
        await().until(() -> configuration.getLong() == 123);
    }

    @Test
    public void testDouble() {
        assertThat(configuration.getDouble()).isEqualTo(3.3D);
        environment.setProperty("annotationPrefix.double", "4.4");
        await().until(() -> configuration.getDouble() == 4.4);
    }

    @Test
    public void testFloat() {
        assertThat(configuration.getFloat()).isEqualTo(4.5F);
        environment.setProperty("annotationPrefix.float", "5.5");
        await().until(() -> configuration.getFloat() == 5.5F);
    }

    @Test
    public void testBoolean() {
        assertThat(configuration.getBoolean()).isTrue();
        environment.setProperty("annotationPrefix.boolean", "false");
        await().until(() -> !configuration.getBoolean());
    }

    @Test
    public void testDuration() {
        assertThat(configuration.getDuration()).isEqualTo(Duration.ofSeconds(60));
        environment.setProperty("annotationPrefix.duration", "24h");
        assertThat(configuration.getDuration()).isEqualTo(Duration.ofDays(1));
    }

    @Test
    public void testList() {
        assertThat(configuration.getList()).containsExactly("a", "b", "c");
        environment.setProperty("annotationPrefix.list", "A,B,C");
        List<String> expected = Arrays.asList("A", "B", "C");
        await().until(() -> configuration.getList().equals(expected));
    }

    @Test
    public void testEmptyList() {
        assertThat(configuration.getEmptyList()).isEmpty();
    }

    @Test
    public void testSet() {
        assertThat(configuration.getSet()).containsExactly("d", "e", "f");
        environment.setProperty("annotationPrefix.set", "D,E,F");
        Set<String> expected = CollectionsExt.asSet("D", "E", "F");
        await().until(() -> configuration.getSet().containsAll(expected));
    }

    @Test
    public void testEmptySet() {
        assertThat(configuration.getEmptySet()).isEmpty();
    }

    @Test
    public void testNullValue() {
        assertThat(configuration.getInteger()).isNull();
        environment.setProperty("annotationPrefix.integer", "1");
        await().until(() -> configuration.getInteger() != null && configuration.getInteger() == 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateFailsOnPropertiesWithoutValue() {
        Archaius2Ext.newConfiguration(SomeConfiguration.class, MyEnvironments.newMutable());
    }

    @Test
    public void testCustomPrefix() {
        environment.setProperty("customPrefix.intWithNoDefault", "456");
        SomeConfiguration configuration = Archaius2Ext.newConfiguration(SomeConfiguration.class, "customPrefix", environment);
        assertThat(configuration.getIntWithNoDefault()).isEqualTo(456);
    }

    @Test
    public void testDefaultMethods() {
        assertThat(configuration.getNumber(false)).isEqualTo(1);
        assertThat(configuration.getNumber(true)).isEqualTo(2);
    }

    @Test
    public void testPropertyNameOverride() {
        assertThat(configuration.getMyStrangeName()).isEqualTo("strangeDefault");
        environment.setProperty("annotationPrefix.my.strange.name", "abc");
        assertThat(configuration.getMyStrangeName()).isEqualTo("abc");
    }

    @Test
    public void testToString() {
        String[] expectedParts = {"float=4.5", "boolean=true", "double=3.3", "intWithNoDefault=11", "list=[a, b, c]", "set=[d, e, f]", "int=1", "long=2"};
        String actual = configuration.toString();
        for (String part : expectedParts) {
            assertThat(actual).contains(part);
        }
    }

    @Configuration(prefix = "annotationPrefix")
    private interface SomeConfiguration {

        @DefaultValue("1")
        int getInt();

        Integer getInteger();

        int getIntWithNoDefault();

        @DefaultValue("2")
        long getLong();

        @DefaultValue("3.3")
        double getDouble();

        @DefaultValue("4.5")
        double getFloat();

        @DefaultValue("true")
        boolean getBoolean();

        @DefaultValue("60s")
        Duration getDuration();

        @DefaultValue("a,b,c")
        List<String> getList();

        List<String> getEmptyList();

        @DefaultValue("d,e,f")
        List<String> getSet();

        Set<String> getEmptySet();

        @DefaultValue("strangeDefault")
        @PropertyName(name = "my.strange.name")
        String getMyStrangeName();

        default long getNumber(boolean returnLong) {
            return returnLong ? getLong() : getInt();
        }
    }
}