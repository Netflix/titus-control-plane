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

package com.netflix.titus.common.util.concurrency;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class CallbackCountDownLatchTest {

    @Test
    public void zeroSizeIsValid() {
        final AtomicBoolean called = new AtomicBoolean(false);
        new CallbackCountDownLatch(0, () -> called.set(true));
        Assert.assertTrue(called.get());
    }

    @Test
    public void zeroCountDownTriggersCallback() {
        final AtomicInteger timesCalled = new AtomicInteger(0);
        final CallbackCountDownLatch latch = new CallbackCountDownLatch(2, timesCalled::getAndIncrement);
        assertThat(timesCalled).hasValue(0);
        latch.countDown();
        assertThat(timesCalled).hasValue(0);
        latch.countDown();
        assertThat(timesCalled).hasValue(1);

        // only called once
        latch.countDown();
        assertThat(timesCalled).hasValue(1);
    }

    @Test
    public void asyncCallback() {
        final Executor mockExecutor = mock(Executor.class);
        final Runnable noop = () -> {
        };
        final CallbackCountDownLatch latch = new CallbackCountDownLatch(2, noop, mockExecutor);
        verify(mockExecutor, never()).execute(any());
        latch.countDown();
        verify(mockExecutor, never()).execute(any());
        latch.countDown();
        verify(mockExecutor).execute(any());

        // only called once
        latch.countDown();
        verify(mockExecutor).execute(any());
    }
}