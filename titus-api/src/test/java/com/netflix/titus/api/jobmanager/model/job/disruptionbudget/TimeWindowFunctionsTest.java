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

package com.netflix.titus.api.jobmanager.model.job.disruptionbudget;

import java.time.DayOfWeek;
import java.time.Month;
import java.util.function.Supplier;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.common.util.time.TestClock;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TimeWindowFunctionsTest {

    private final TestClock clock = Clocks.testWorldClock(2000, Month.JANUARY, 1);

    private final TitusRuntime titusRuntime = TitusRuntimes.test(clock);

    @Test
    public void testEmptyTimeWindowPredicate() {
        Supplier<Boolean> predicate = TimeWindowFunctions.isInTimeWindowPredicate(titusRuntime, TimeWindow.empty());
        assertThat(predicate.get()).isTrue();
    }

    @Test
    public void testTimeWindowPredicateInUtc() {
        TimeWindow timeWindow = TimeWindow.newBuilder()
                .withDays(Day.Monday)
                .withwithHourlyTimeWindows(8, 16)
                .build();
        Supplier<Boolean> predicate = TimeWindowFunctions.isInTimeWindowPredicate(titusRuntime, timeWindow);

        // Before 8
        clock.jumpForwardTo(DayOfWeek.MONDAY).resetTime(7, 0, 0);
        assertThat(predicate.get()).isFalse();

        // During the day
        clock.resetTime(11, 0, 0);
        assertThat(predicate.get()).isTrue();

        // After 16
        clock.resetTime(17, 0, 0);
        assertThat(predicate.get()).isFalse();
    }

    @Test
    public void testTimeWindowPredicateInPst() {
        TimeWindow timeWindow = TimeWindow.newBuilder()
                .withDays(Day.Monday)
                .withwithHourlyTimeWindows(8, 16)
                .withTimeZone("PST")
                .build();
        Supplier<Boolean> predicate = TimeWindowFunctions.isInTimeWindowPredicate(titusRuntime, timeWindow);

        // 8am UTC (== 0am PST)
        clock.jumpForwardTo(DayOfWeek.MONDAY).resetTime(10, 0, 0);
        assertThat(predicate.get()).isFalse();

        // 4pm UTC (== 8am PST)
        clock.resetTime(16, 0, 0);
        assertThat(predicate.get()).isTrue();

        // 8pm UTC (== 12pm PST)
        clock.resetTime(20, 0, 0);
    }

    @Test
    public void testMultipleTimeWindowsPredicate() {
        TimeWindow timeWindow1 = TimeWindow.newBuilder()
                .withDays(Day.Monday, Day.Tuesday, Day.Wednesday, Day.Thursday)
                .withwithHourlyTimeWindows(8, 11, 13, 16)
                .build();
        TimeWindow timeWindow2 = TimeWindow.newBuilder()
                .withDays(Day.Friday)
                .withwithHourlyTimeWindows(8, 13)
                .build();
        Supplier<Boolean> predicate = TimeWindowFunctions.isInTimeWindowPredicate(titusRuntime, asList(timeWindow1, timeWindow2));

        // Wednesday
        clock.jumpForwardTo(DayOfWeek.WEDNESDAY).resetTime(7, 0, 0);
        assertThat(predicate.get()).isFalse();
        clock.jumpForwardTo(DayOfWeek.WEDNESDAY).resetTime(10, 0, 0);
        assertThat(predicate.get()).isTrue();
        clock.jumpForwardTo(DayOfWeek.WEDNESDAY).resetTime(12, 0, 0);
        assertThat(predicate.get()).isFalse();
        clock.jumpForwardTo(DayOfWeek.WEDNESDAY).resetTime(15, 0, 0);
        assertThat(predicate.get()).isTrue();
        clock.jumpForwardTo(DayOfWeek.WEDNESDAY).resetTime(18, 0, 0);
        assertThat(predicate.get()).isFalse();

        // Friday
        clock.jumpForwardTo(DayOfWeek.FRIDAY).resetTime(7, 0, 0);
        assertThat(predicate.get()).isFalse();
        clock.jumpForwardTo(DayOfWeek.FRIDAY).resetTime(12, 0, 0);
        assertThat(predicate.get()).isTrue();
        clock.jumpForwardTo(DayOfWeek.FRIDAY).resetTime(15, 0, 0);
        assertThat(predicate.get()).isFalse();
    }
}