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
import java.time.LocalDateTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

class TestWorldClock implements TestClock {

    private OffsetDateTime dateTime;

    TestWorldClock(int year, Month month, int day) {
        this.dateTime = OffsetDateTime.of(year, month.getValue(), day, 0, 0, 0, 0, ZoneOffset.UTC);
    }

    TestWorldClock() {
        this.dateTime = OffsetDateTime.of(LocalDateTime.now(), ZoneOffset.UTC);
    }

    @Override
    public long advanceTime(long interval, TimeUnit timeUnit) {
        this.dateTime = dateTime.plusNanos(timeUnit.toNanos(interval));
        return wallTime();
    }

    @Override
    public TestClock resetDate(int year, Month month, int dayOfMonth) {
        this.dateTime = dateTime.withYear(year).withMonth(month.getValue()).withDayOfMonth(dayOfMonth);
        return this;
    }

    @Override
    public TestClock resetTime(int hour, int minute, int second) {
        this.dateTime = dateTime.withHour(hour).withMinute(minute).withSecond(second);
        return this;
    }

    @Override
    public TestClock jumpForwardTo(DayOfWeek dayOfWeek) {
        int expected = dayOfWeek.getValue();
        int current = dateTime.getDayOfWeek().getValue();

        if (current == expected) {
            return this;
        }

        int plusDays = expected > current
                ? expected - current
                : 7 - (current - expected);
        this.dateTime = dateTime.plusDays(plusDays);

        return this;
    }

    @Override
    public long nanoTime() {
        throw new IllegalStateException("Method not supported");
    }

    @Override
    public long wallTime() {
        return dateTime.toEpochSecond() * 1_000;
    }
}
