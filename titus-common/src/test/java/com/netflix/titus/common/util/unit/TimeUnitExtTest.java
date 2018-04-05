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

package com.netflix.titus.common.util.unit;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TimeUnitExtTest {

    @Test
    public void testParse() {
        testParse("1ms", 1, TimeUnit.MILLISECONDS);
        testParse("2s", 2, TimeUnit.SECONDS);
        testParse("3m", 3, TimeUnit.MINUTES);
        testParse("4h", 4, TimeUnit.HOURS);
        testParse("5d", 5, TimeUnit.DAYS);
    }

    @Test
    public void testParseWithInvalidData() {
        assertThat(TimeUnitExt.parse("1b2mss")).isNotPresent();
    }

    @Test
    public void testToMillis() {
        assertThat(TimeUnitExt.toMillis("1m").get()).isEqualTo(60_000);
    }

    private void testParse(String value, long expectedInterval, TimeUnit expectedUnit) {
        Optional<Pair<Long, TimeUnit>> result = TimeUnitExt.parse(value);
        assertThat(result).isPresent();
        assertThat(result.get().getLeft()).isEqualTo(expectedInterval);
        assertThat(result.get().getRight()).isEqualTo(expectedUnit);
    }
}