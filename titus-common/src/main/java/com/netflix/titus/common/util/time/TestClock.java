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

import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Month;
import java.util.concurrent.TimeUnit;

/**
 */
public interface TestClock extends Clock {

    long advanceTime(long interval, TimeUnit timeUnit);

    default long advanceTime(Duration duration) {
        return advanceTime(duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    default TestClock resetDate(int year, Month month, int day) {
        throw new IllegalStateException("method not supported");
    }

    default TestClock resetTime(int hour, int minute, int second) {
        throw new IllegalStateException("method not supported");
    }

    default TestClock jumpForwardTo(DayOfWeek dayOfWeek) {
        throw new IllegalStateException("method not supported");
    }
}
