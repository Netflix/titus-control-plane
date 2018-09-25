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

package com.netflix.titus.common.jhiccup.sensor;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.jhiccup.HiccupRecorderConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RxJavaComputationSchedulerSensorTest {

    private static final long PROBING_INTERVAL_MS = 10;
    private static final long PROGRESS_CHECK_INTERVAL_MS = 10;

    private final HiccupRecorderConfiguration configuration = mock(HiccupRecorderConfiguration.class);

    private RxJavaComputationSchedulerSensor sensor;
    private final AtomicReference<StackTraceElement[]> blockedThreadRef = new AtomicReference<>();

    @Before
    public void setUp() {
        when(configuration.getTaskExecutionDeadlineMs()).thenReturn(100L);

        sensor = new RxJavaComputationSchedulerSensor(configuration, PROBING_INTERVAL_MS, PROGRESS_CHECK_INTERVAL_MS, new DefaultRegistry()) {
            @Override
            protected void reportBlockedThread(Thread blockedThread) {
                blockedThreadRef.set(blockedThread.getStackTrace());
            }
        };
    }

    @After
    public void tearDown() {
        if (sensor != null) {
            sensor.shutdown();
        }
    }

    @Test
    public void testExpectedNumberOfComputationThreadsIsDetected() {
        assertThat(sensor.getLastPercentile(99.5).keySet()).hasSize(Runtime.getRuntime().availableProcessors());
    }

    @Test(timeout = 30_000)
    public void testLongRunningTask() throws Exception {
        Exception lastError = null;
        for (int multiplier = 2; multiplier < 5; multiplier++) {
            try {
                tryTestLongRunningTask(multiplier);
                lastError = null;
                break;
            } catch (Exception e) {
                lastError = e;
            }
        }
        if (lastError != null) {
            throw lastError;
        }
    }

    // To avoid a long pause in the unit test, we try first with a short delay, and the risk of detection miss.
    // If the latter happens, the delay is increased and the test is repeated.
    private void tryTestLongRunningTask(int delayMultiplier) throws InterruptedException {
        Scheduler.Worker worker = Schedulers.computation().createWorker();
        CountDownLatch latch = new CountDownLatch(1);
        try {
            worker.schedule(() -> {
                try {
                    Thread.sleep(PROBING_INTERVAL_MS * delayMultiplier);
                } catch (InterruptedException ignore) {
                }
                latch.countDown();
            });
            latch.await();
        } finally {
            worker.unsubscribe();
        }

        // Now give some time for pending tasks to complete
        await().pollDelay(1, TimeUnit.MILLISECONDS)
                .timeout(2 * delayMultiplier * PROBING_INTERVAL_MS, TimeUnit.MILLISECONDS)
                .until(() -> {
                    Map<String, Long> percentiles = sensor.getLastPercentile(99.99);
                    long paused = percentiles.entrySet().stream().filter(e -> e.getValue() >= PROBING_INTERVAL_MS).count();
                    return paused > 0;
                });
    }

    @Test(timeout = 30_000)
    public void testBlockedThreadsDetection() throws Exception {
        Scheduler.Worker worker = Schedulers.computation().createWorker();
        CountDownLatch latch = new CountDownLatch(1);
        try {
            worker.schedule(() -> {
                try {
                    while (blockedThreadRef.get() == null) {
                        Thread.sleep(1);
                    }
                } catch (InterruptedException ignore) {
                }
                latch.countDown();
            });
            latch.await();
        } finally {
            worker.unsubscribe();
        }

        Optional<StackTraceElement> ourElement = Arrays.stream(blockedThreadRef.get())
                .filter(l -> l.getClassName().contains(RxJavaComputationSchedulerSensorTest.class.getSimpleName()))
                .findFirst();
        assertThat(ourElement).isPresent();
    }
}