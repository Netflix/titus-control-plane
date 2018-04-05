/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.common.util.rx;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.functions.Action0;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializedTaskInvokerTest {

    private static final Action0 DO_NOTHING = () -> {
    };

    private final TestScheduler testScheduler = Schedulers.test();

    private final SerializedTaskInvoker serializedTaskInvoker = new SerializedTaskInvoker(testScheduler);

    @Test
    public void testOrdering() throws Exception {
        List<Integer> collector = new CopyOnWriteArrayList<>();

        ExtTestSubscriber<Void> firstSubscriber = new ExtTestSubscriber<>();
        serializedTaskInvoker.submit(() -> collector.add(1)).subscribe(firstSubscriber);

        ExtTestSubscriber<Void> secondSubscriber = new ExtTestSubscriber<>();
        serializedTaskInvoker.submit(() -> collector.add(2)).subscribe(secondSubscriber);

        testScheduler.triggerActions();
        firstSubscriber.assertOnCompleted();
        secondSubscriber.assertOnCompleted();

        assertThat(collector).containsExactly(1, 2);

        serializedTaskInvoker.shutdown();
        assertThat(serializedTaskInvoker.activeSubscribers).isEmpty();
    }

    @Test
    public void testShutdownWithPendingTasks() throws Exception {
        ExtTestSubscriber<Void> firstSubscriber = new ExtTestSubscriber<>();
        serializedTaskInvoker.submit(DO_NOTHING).subscribe(firstSubscriber);

        serializedTaskInvoker.shutdown();
        testScheduler.triggerActions();

        assertThat(serializedTaskInvoker.activeSubscribers).isEmpty();
        firstSubscriber.assertOnError(Exception.class);
    }

    @Test
    public void testSubmitAfterShutdown() throws Exception {
        serializedTaskInvoker.shutdown();

        ExtTestSubscriber<Void> firstSubscriber = new ExtTestSubscriber<>();
        serializedTaskInvoker.submit(DO_NOTHING).subscribe(firstSubscriber);

        assertThat(serializedTaskInvoker.activeSubscribers).isEmpty();
        firstSubscriber.assertOnError(Exception.class);
    }
}