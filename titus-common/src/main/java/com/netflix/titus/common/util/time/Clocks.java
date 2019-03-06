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

package com.netflix.titus.common.util.time;

import java.time.Month;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.util.time.internal.DefaultTestClock;
import com.netflix.titus.common.util.time.internal.SystemClock;
import rx.Scheduler;
import rx.schedulers.TestScheduler;

/**
 */
public class Clocks {

    public static Clock system() {
        return SystemClock.INSTANCE;
    }

    public static Clock scheduler(Scheduler scheduler) {
        return new Clock() {
            @Override
            public long nanoTime() {
                return TimeUnit.MILLISECONDS.toNanos(scheduler.now());
            }

            @Override
            public long wallTime() {
                return scheduler.now();
            }
        };
    }

    public static TestClock test() {
        return new DefaultTestClock();
    }

    public static TestClock testWorldClock() {
        return new TestWorldClock("UTC");
    }

    public static TestClock testWorldClock(String zoneId) {
        return new TestWorldClock(zoneId);
    }

    public static TestClock testWorldClock(int year, Month month, int day) {
        return new TestWorldClock("UTC", year, month, day);
    }

    public static TestClock testScheduler(TestScheduler testScheduler) {
        return new TestClock() {
            @Override
            public long advanceTime(long interval, TimeUnit timeUnit) {
                testScheduler.advanceTimeBy(interval, timeUnit);
                return testScheduler.now();
            }

            @Override
            public long nanoTime() {
                return TimeUnit.MILLISECONDS.toNanos(testScheduler.now());
            }

            @Override
            public long wallTime() {
                return testScheduler.now();
            }
        };
    }
}
