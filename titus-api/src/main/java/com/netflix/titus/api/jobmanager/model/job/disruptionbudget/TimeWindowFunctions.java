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
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.netflix.titus.common.util.DateTimeExt;
import com.netflix.titus.common.util.time.Clock;

/**
 * Collection of helper functions for {@link TimeWindow}.
 */
public final class TimeWindowFunctions {

    public static boolean isEmpty(TimeWindow timeWindow) {
        return timeWindow.getDays().isEmpty() && timeWindow.getHourlyTimeWindows().isEmpty();
    }

    /**
     * Returns predicate which when evaluated checks the current time against the defined time window.
     * If the time window is empty (does not define any days our hours), it matches any time.
     *
     * @returns true if the current time is within the time window, false otherwise
     */
    public static Supplier<Boolean> isInTimeWindowPredicate(Clock clock, TimeWindow timeWindow) {
        if (isEmpty(timeWindow)) {
            return () -> true;
        }

        List<Function<DayOfWeek, Boolean>> dayPredicates = new ArrayList<>();
        timeWindow.getDays().forEach(day -> dayPredicates.add(buildDayPredicate(day)));

        List<Function<Integer, Boolean>> hourPredicates = new ArrayList<>();
        timeWindow.getHourlyTimeWindows().forEach(h -> hourPredicates.add(buildHourlyTimeWindows(h)));

        Function<DayOfWeek, Boolean> combinedDayPredicate = dayPredicates.isEmpty() ? day -> true : oneOf(dayPredicates);
        Function<Integer, Boolean> combinedHourPredicate = hourPredicates.isEmpty() ? hour -> true : oneOf(hourPredicates);

        return () -> {
            OffsetDateTime dateTime = DateTimeExt.toDateTimeUTC(clock.wallTime());
            return combinedDayPredicate.apply(dateTime.getDayOfWeek()) && combinedHourPredicate.apply(dateTime.getHour());
        };
    }

    /**
     * Returns predicate that evaluates to true only when {@link #isInTimeWindowPredicate(Clock, TimeWindow)} evaluates
     * to true for at least one of the provided time windows.
     */
    public static Supplier<Boolean> isInTimeWindowPredicate(Clock clock, Collection<TimeWindow> timeWindows) {
        List<Supplier<Boolean>> predicates = timeWindows.stream()
                .map(t -> isInTimeWindowPredicate(clock, t))
                .collect(Collectors.toList());
        return () -> {
            for (Supplier<Boolean> predicate : predicates) {
                if (predicate.get()) {
                    return true;
                }
            }
            return false;
        };
    }

    private static <T> Function<T, Boolean> oneOf(List<Function<T, Boolean>> basicPredicates) {
        return argument -> {
            if (basicPredicates.isEmpty()) {
                return true;
            }

            for (Function<T, Boolean> predicate : basicPredicates) {
                if (predicate.apply(argument)) {
                    return true;
                }
            }
            return false;
        };
    }

    private static Function<DayOfWeek, Boolean> buildDayPredicate(Day expectedDay) {
        DayOfWeek expectedDayOfWeek = expectedDay.toDayOfWeek();
        return currentDayOfWeek -> currentDayOfWeek == expectedDayOfWeek;
    }

    private static Function<Integer, Boolean> buildHourlyTimeWindows(HourlyTimeWindow timeWindow) {
        if (timeWindow.getEndHour() < timeWindow.getStartHour()) {
            return epochMs -> true;
        }
        return currentHour -> timeWindow.getStartHour() <= currentHour && currentHour <= timeWindow.getEndHour();
    }
}
