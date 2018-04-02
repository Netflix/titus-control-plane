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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class DateTimeExtTest {

    @Test
    public void testUtcDateTimeStringConversion() {
        ZonedDateTime now = ZonedDateTime.now();
        String expected = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.of("UTC")).format(now) + 'Z';

        String actual = DateTimeExt.toUtcDateTimeString(Instant.from(now).toEpochMilli());
        assertThat(actual, is(equalTo(expected)));
    }

    @Test
    public void testTime() throws Exception {
        long msDuration = 123;
        assertThat(DateTimeExt.toTimeUnitString(msDuration), is(equalTo("123ms")));

        long secDuration = 3 * 1000 + 123;
        assertThat(DateTimeExt.toTimeUnitString(secDuration), is(equalTo("3s 123ms")));

        long minDuration = (2 * 60 + 3) * 1000 + 123;
        assertThat(DateTimeExt.toTimeUnitString(minDuration), is(equalTo("2min 3s 123ms")));

        long hourDuration = ((1 * 60 + 2) * 60 + 3) * 1000 + 123;
        assertThat(DateTimeExt.toTimeUnitString(hourDuration), is(equalTo("1h 2min 3s 123ms")));

        long oneSecond = 1 * 1000;
        assertThat(DateTimeExt.toTimeUnitString(oneSecond), is(equalTo("1s")));

        long oneMillis = 1;
        assertThat(DateTimeExt.toTimeUnitString(oneMillis), is(equalTo("1ms")));

        long zeroMillis = 0;
        assertThat(DateTimeExt.toTimeUnitString(zeroMillis), is(equalTo("0ms")));

        long twoDays = TimeUnit.DAYS.toMillis(2);
        assertThat(DateTimeExt.toTimeUnitString(twoDays), is(equalTo("2d")));
    }
}