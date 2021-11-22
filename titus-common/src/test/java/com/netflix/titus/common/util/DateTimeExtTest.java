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
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static com.netflix.titus.common.util.DateTimeExt.toRateString;
import static com.netflix.titus.common.util.DateTimeExt.toTimeUnitString;
import static org.assertj.core.api.Assertions.assertThat;

public class DateTimeExtTest {

    @Test
    public void testUtcDateTimeStringConversion() {
        ZonedDateTime now = ZonedDateTime.now();
        String expected = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("UTC")).format(now) + 'Z';

        String actual = DateTimeExt.toUtcDateTimeString(Instant.from(now).toEpochMilli());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testTime() {
        long msDuration = 123;
        assertThat(toTimeUnitString(msDuration)).isEqualTo("123ms");

        long secDuration = 3 * 1000 + 123;
        assertThat(toTimeUnitString(secDuration)).isEqualTo("3s 123ms");

        long minDuration = (2 * 60 + 3) * 1000 + 123;
        assertThat(toTimeUnitString(minDuration)).isEqualTo("2min 3s 123ms");

        long hourDuration = ((1 * 60 + 2) * 60 + 3) * 1000 + 123;
        assertThat(toTimeUnitString(hourDuration)).isEqualTo("1h 2min 3s 123ms");

        long oneSecond = 1 * 1000;
        assertThat(toTimeUnitString(oneSecond)).isEqualTo("1s");

        long oneMillis = 1;
        assertThat(toTimeUnitString(oneMillis)).isEqualTo("1ms");

        long zeroMillis = 0;
        assertThat(toTimeUnitString(zeroMillis)).isEqualTo("0ms");

        long twoDays = TimeUnit.DAYS.toMillis(2);
        assertThat(toTimeUnitString(twoDays)).isEqualTo("2d");
    }

    @Test
    public void testRoundedTime() {
        long msDuration = 123;
        assertThat(toTimeUnitString(msDuration, 2)).isEqualTo("123ms");

        long secDuration = 3 * 1000 + 123;
        assertThat(toTimeUnitString(secDuration, 1)).isEqualTo("3s");
        assertThat(toTimeUnitString(secDuration, 2)).isEqualTo("3s 123ms");

        long minDuration = (2 * 60 + 3) * 1000 + 123;
        assertThat(toTimeUnitString(minDuration, 1)).isEqualTo("2min");
        assertThat(toTimeUnitString(minDuration, 2)).isEqualTo("2min 3s");
        assertThat(toTimeUnitString(minDuration, 3)).isEqualTo("2min 3s 123ms");

        long hourDuration = ((1 * 60 + 2) * 60 + 3) * 1000 + 123;
        assertThat(toTimeUnitString(hourDuration, 1)).isEqualTo("1h");
        assertThat(toTimeUnitString(hourDuration, 2)).isEqualTo("1h 2min");
        assertThat(toTimeUnitString(hourDuration, 3)).isEqualTo("1h 2min 3s");
        assertThat(toTimeUnitString(hourDuration, 4)).isEqualTo("1h 2min 3s 123ms");

        long oneSecond = 1 * 1000;
        assertThat(toTimeUnitString(oneSecond, 2)).isEqualTo("1s");

        long oneMillis = 1;
        assertThat(toTimeUnitString(oneMillis, 2)).isEqualTo("1ms");

        long zeroMillis = 0;
        assertThat(toTimeUnitString(zeroMillis, 2)).isEqualTo("0ms");

        long twoDays = TimeUnit.DAYS.toMillis(2);
        assertThat(toTimeUnitString(twoDays, 2)).isEqualTo("2d");
    }

    @Test
    public void testToRateString() {
        assertThat(toRateString(1, 1, TimeUnit.MILLISECONDS, "action")).isEqualTo("1.00 action/ms");
        assertThat(toRateString(60, 5, TimeUnit.SECONDS, "action")).isEqualTo("5.00 action/min");
    }

    @Test
    public void testFromMillis() {
        long timestamp = 1234;
        OffsetDateTime dateTime = DateTimeExt.fromMillis(timestamp);
        assertThat(dateTime.toInstant().toEpochMilli()).isEqualTo(timestamp);
    }
}