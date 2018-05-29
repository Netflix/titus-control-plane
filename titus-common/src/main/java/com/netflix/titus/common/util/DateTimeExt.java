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

package com.netflix.titus.common.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.google.protobuf.Duration;
import com.google.protobuf.util.Durations;

/**
 * Data and time supplementary functions.
 */
public final class DateTimeExt {

    private static final DateTimeFormatter ISO_UTC_DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("UTC"));

    private DateTimeExt() {
    }

    /**
     * The time given in the argument is scoped to a local (system default) time zone. The result
     * is adjusted to UTC time zone.
     */
    public static String toUtcDateTimeString(long msSinceEpoch) {
        if (msSinceEpoch == 0L) {
            return null;
        }
        return ISO_UTC_DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(msSinceEpoch)) + 'Z';
    }

    /**
     * Given a duration with milliseconds resolution, format it using time units.
     * For example 3600,000 is formatted as 1h.
     */
    public static String toTimeUnitString(Duration duration) {
        return toTimeUnitString(Durations.toMillis(duration));
    }

    /**
     * Given a duration in milliseconds, format it using time units. For example 3600,000 is formatted as 1h.
     */
    public static String toTimeUnitString(long timeMs) {
        StringBuilder sb = new StringBuilder();

        long sec = timeMs / 1000;
        long min = sec / 60;
        long hour = min / 60;
        long day = hour / 24;

        if (day > 0) {
            sb.append(' ').append(day).append("d");
        }
        if (hour % 24 > 0) {
            sb.append(' ').append(hour % 24).append("h");
        }
        if (min % 60 > 0) {
            sb.append(' ').append(min % 60).append("min");
        }
        if (sec % 60 > 0) {
            sb.append(' ').append(sec % 60).append("s");
        }
        if (timeMs % 1000 > 0) {
            sb.append(' ').append(timeMs % 1000).append("ms");
        }
        if (sb.length() == 0) {
            return "0ms";
        } else if (sb.charAt(0) == ' ') {
            return sb.substring(1);
        }
        return sb.toString();
    }
}
