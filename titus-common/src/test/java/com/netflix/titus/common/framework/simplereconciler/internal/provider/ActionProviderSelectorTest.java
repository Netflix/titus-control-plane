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

package com.netflix.titus.common.framework.simplereconciler.internal.provider;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import com.netflix.titus.common.framework.simplereconciler.ReconcilerActionProvider;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.common.framework.simplereconciler.internal.provider.SampleActionProviderCatalog.newExternalProvider;
import static com.netflix.titus.common.framework.simplereconciler.internal.provider.SampleActionProviderCatalog.newInternalProvider;
import static org.assertj.core.api.Assertions.assertThat;

public class ActionProviderSelectorTest {

    private final TestClock testClock = Clocks.test();
    private final TitusRuntime titusRuntime = TitusRuntimes.test(testClock);

    @Before
    public void setUp() throws Exception {
        testClock.advanceTime(Duration.ofMillis(1_000));
    }

    @Test
    public void testSingleProvider() {
        ActionProviderSelector<String> selector = new ActionProviderSelector<>("test",
                Collections.singletonList(newExternalProvider("a")), titusRuntime);
        Iterator<ReconcilerActionProvider<String>> it = selector.next(testClock.wallTime());
        expectExternal(it, "a");
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    public void testTwoProvider() {
        ActionProviderSelector<String> selector = new ActionProviderSelector<>("test", Arrays.asList(
                newExternalProvider("a"),
                newInternalProvider("b", 1_000)
        ), titusRuntime);
        Iterator<ReconcilerActionProvider<String>> it = selector.next(testClock.wallTime());
        expectExternal(it, "a");
        expectInternal(it, "b");
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    public void testManyProvider() {
        ActionProviderSelector<String> selector = new ActionProviderSelector<>("test", Arrays.asList(
                newExternalProvider("a"),
                newInternalProvider("b", 2_000),
                newInternalProvider("c", 1_000)
        ), titusRuntime);
        Iterator<ReconcilerActionProvider<String>> it = selector.next(testClock.wallTime());
        expectExternal(it, "a");
        expectInternal(it, "c");
        expectInternal(it, "b");
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    public void testProviderWithExecutionInterval() {
        ActionProviderSelector<String> selector = new ActionProviderSelector<>("test", Arrays.asList(
                newExternalProvider("a"),
                newInternalProvider("b", 1, Duration.ofMillis(1_000), Duration.ZERO)
        ), titusRuntime);
        testClock.advanceTime(Duration.ofMillis(10_000));

        Iterator<ReconcilerActionProvider<String>> it = selector.next(testClock.wallTime());
        expectInternal(it, "b");
        expectExternal(it, "a");
        assertThat(it.hasNext()).isFalse();
        selector.updateEvaluationTime("b", testClock.wallTime());

        it = selector.next(testClock.wallTime());
        expectExternal(it, "a");
        assertThat(it.hasNext()).isFalse();

        testClock.advanceTime(Duration.ofMillis(1_000));
    }

    @Test
    public void testProviderWithMinExecutionInterval() {
        ActionProviderSelector<String> selector = new ActionProviderSelector<>("test", Arrays.asList(
                newExternalProvider("a"),
                newInternalProvider("b", 1_000, Duration.ZERO, Duration.ofMillis(10_000))
        ), titusRuntime);
        Iterator<ReconcilerActionProvider<String>> it = selector.next(testClock.wallTime());
        expectExternal(it, "a");
        expectInternal(it, "b");
        assertThat(it.hasNext()).isFalse();

        testClock.advanceTime(Duration.ofMillis(10_000));
        it = selector.next(testClock.wallTime());
        expectInternal(it, "b");
        expectExternal(it, "a");
        assertThat(it.hasNext()).isFalse();
        selector.updateEvaluationTime("b", 10_000);

        it = selector.next(testClock.wallTime());
        expectExternal(it, "a");
        expectInternal(it, "b");
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    public void testTwoProvidersWithSamePriority() {
        ActionProviderSelector<String> selector = new ActionProviderSelector<>("test", Arrays.asList(
                newInternalProvider("a", 1_000, Duration.ZERO, Duration.ZERO),
                newInternalProvider("b", 1_000, Duration.ZERO, Duration.ZERO)
        ), titusRuntime);
        Iterator<ReconcilerActionProvider<String>> it = selector.next(testClock.wallTime());
        ReconcilerActionProvider<String> next = it.next();
        if (next.getPolicy().getName().equals("a")) {
            expectInternal(it, "b");
            selector.updateEvaluationTime("a", testClock.wallTime());
            it = selector.next(testClock.wallTime());
            expectInternal(it, "b");
            expectInternal(it, "a");
        } else {
            expectInternal(it, "a");
            selector.updateEvaluationTime("b", testClock.wallTime());
            it = selector.next(testClock.wallTime());
            expectInternal(it, "a");
            expectInternal(it, "b");
        }
    }

    private void expect(Iterator<ReconcilerActionProvider<String>> it, String name, boolean external) {
        assertThat(it.hasNext()).isTrue();
        ReconcilerActionProvider<String> next = it.next();
        assertThat(next.getPolicy().getName()).isEqualTo(name);
        assertThat(next.isExternal()).isEqualTo(external);
    }

    private void expectExternal(Iterator<ReconcilerActionProvider<String>> it, String name) {
        expect(it, name, true);
    }

    private void expectInternal(Iterator<ReconcilerActionProvider<String>> it, String name) {
        expect(it, name, false);
    }
}