/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.util.spectator;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SpectatorExtTest {

    @Test
    public void testUniqueTagValue() {
        assertThat(SpectatorExt.uniqueTagValue("testBase", value -> false)).isEqualTo("testBase");
        assertThat(SpectatorExt.uniqueTagValue("testBase", value -> value.equals("testBase"))).startsWith("testBase#");
        AtomicInteger counter = new AtomicInteger();
        assertThat(SpectatorExt.uniqueTagValue("testBase", value -> counter.incrementAndGet() < 5)).startsWith("testBase#");
        assertThat(counter).hasValue(5);

        String longValue = buildLongValue();
        assertThat(SpectatorExt.uniqueTagValue(longValue, value -> false)).hasSize(SpectatorExt.MAX_TAG_VALUE_LENGTH);
        assertThat(SpectatorExt.uniqueTagValue(longValue, value -> !value.contains("#"))).hasSize(SpectatorExt.MAX_TAG_VALUE_LENGTH);
    }

    private String buildLongValue() {
        StringBuilder longValueBuilder = new StringBuilder();
        for (int i = 0; i < SpectatorExt.MAX_TAG_VALUE_LENGTH; i++) {
            longValueBuilder.append("X");
        }
        return longValueBuilder.append("END").toString();
    }
}