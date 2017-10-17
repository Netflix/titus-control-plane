/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.framework.reconciler.internal;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;
import io.netflix.titus.common.framework.reconciler.EntityHolder;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine.TriggerStatus;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultReconciliationFrameworkTest {

    private static final long IDLE_TIMEOUT_MS = 100;
    private static final long ACTIVE_TIMEOUT_MS = 20;
    private static final int STOP_TIMEOUT_MS = 1_000;

    private final TestScheduler testScheduler = Schedulers.test();

    private final Function<EntityHolder, ReconciliationEngine<String>> engineFactory = mock(Function.class);
    private final ReconciliationEngine engine1 = mock(ReconciliationEngine.class);
    private final ReconciliationEngine engine2 = mock(ReconciliationEngine.class);

    private final Map<Object, Comparator<EntityHolder>> indexComparators = ImmutableMap.<Object, Comparator<EntityHolder>>builder()
            .put("ascending", Comparator.comparing(EntityHolder::getEntity))
            .put("descending", Comparator.<EntityHolder, String>comparing(EntityHolder::getEntity).reversed())
            .build();

    private final DefaultReconciliationFramework<String> framework = new DefaultReconciliationFramework<String>(
            engineFactory,
            IDLE_TIMEOUT_MS,
            ACTIVE_TIMEOUT_MS,
            indexComparators,
            testScheduler
    );

    @Before
    public void setUp() throws Exception {
        framework.start();
        when(engineFactory.apply(any())).thenReturn(engine1, engine2);
        when(engine1.triggerEvents()).thenReturn(new TriggerStatus(true, true));
        when(engine1.getReferenceView()).thenReturn(EntityHolder.newRoot("myRoot1", "myEntity1"));
        when(engine2.triggerEvents()).thenReturn(new TriggerStatus(true, true));
        when(engine2.getReferenceView()).thenReturn(EntityHolder.newRoot("myRoot2", "myEntity2"));
    }

    @After
    public void tearDown() throws Exception {
        framework.stop(STOP_TIMEOUT_MS);
    }

    @Test
    public void testEngineLifecycle() throws Exception {
        ExtTestSubscriber<ReconciliationEngine> addSubscriber = new ExtTestSubscriber<>();
        framework.newEngine(EntityHolder.newRoot("myRoot", "myEntity")).subscribe(addSubscriber);
        testScheduler.triggerActions();

        ReconciliationEngine engine = addSubscriber.takeNext();
        verify(engine, times(1)).triggerEvents();

        ExtTestSubscriber<Void> removeSubscriber = new ExtTestSubscriber<>();
        framework.removeEngine(engine).subscribe(removeSubscriber);
        testScheduler.advanceTimeBy(IDLE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        verify(engine, times(1)).triggerEvents();
    }

    @Test
    public void testIndexes() throws Exception {
        framework.newEngine(EntityHolder.newRoot("myRoot1", "myEntity1")).subscribe();
        framework.newEngine(EntityHolder.newRoot("myRoot2", "myEntity2")).subscribe();
        testScheduler.triggerActions();

        assertThat(framework.orderedView("ascending").stream().map(EntityHolder::getEntity)).containsExactly("myEntity1", "myEntity2");
        assertThat(framework.orderedView("descending").stream().map(EntityHolder::getEntity)).containsExactly("myEntity2", "myEntity1");
    }
}