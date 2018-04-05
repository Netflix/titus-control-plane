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

package com.netflix.titus.master.job.worker;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.job.worker.internal.DefaultWorkerResubmitRateLimiter;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultWorkerResubmitRateLimiterTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final MasterConfiguration config = mock(MasterConfiguration.class);

    private WorkerResubmitRateLimiter rateLimiter;

    @Before
    public void setUp() throws Exception {
        when(config.getWorkerResubmitIntervalSecs()).thenReturn("5:10:20");
        when(config.getExpireWorkerResubmitDelaySecs()).thenReturn(10L);
        when(config.getExpireResubmitDelayExecutionIntervalSecs()).thenReturn(20L);

        rateLimiter = new DefaultWorkerResubmitRateLimiter(config, testScheduler);
    }

    @Test
    public void testGetResubmitAt1() throws Exception {
        String id = "123";
        rateLimiter.delayWorkerResubmit(id, 0, 0);
        long delay = rateLimiter.getResubmitAt(id, 0, 0) - testScheduler.now();
        assertThat(delay, is(equalTo(0L)));

        // Expect resubmits in 5, 10, 20, 20... intervals in the following sequence
        rateLimiter.delayWorkerResubmit(id, 0, 0);
        delay = rateLimiter.getResubmitAt(id, 0, 0) - testScheduler.now();
        assertThat(diffPercent(5000, delay), is(lessThan(1.0)));

        rateLimiter.delayWorkerResubmit(id, 0, 0);
        delay = rateLimiter.getResubmitAt(id, 0, 0) - testScheduler.now();
        assertThat(diffPercent(10000, delay), is(lessThan(1.0)));

        for (int i = 0; i < 3; i++) {
            rateLimiter.delayWorkerResubmit(id, 0, 0);
            delay = rateLimiter.getResubmitAt(id, 0, 0) - testScheduler.now();
            assertThat(diffPercent(20000, delay), is(lessThan(1.0)));
        }
    }

    @Test
    public void testGetResubmitAt2() throws Exception {
        String id = "234";
        rateLimiter.delayWorkerResubmit(id, 1, 1);
        long delay = rateLimiter.getResubmitAt(id, 1, 1) - testScheduler.now();
        assertThat(delay, is(equalTo(0L)));

        rateLimiter.delayWorkerResubmit(id, 1, 1);
        delay = rateLimiter.getResubmitAt(id, 1, 1) - testScheduler.now();
        assertThat(diffPercent(5000, delay), is(lessThan(1.0)));

        rateLimiter.delayWorkerResubmit(id, 1, 1);
        testScheduler.advanceTimeBy(10, TimeUnit.SECONDS);
        delay = rateLimiter.getResubmitAt(id, 1, 1) - testScheduler.now();
        assertThat(delay, is(equalTo(0L)));
    }

    private double diffPercent(double val1, double val2) {
        return Math.abs(val1 - val2) * 100.0 / val1;
    }
}
