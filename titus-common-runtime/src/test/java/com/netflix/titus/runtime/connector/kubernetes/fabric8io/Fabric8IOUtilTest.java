/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.fabric8io;

import java.time.OffsetDateTime;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class Fabric8IOUtilTest {

    private static final String TIMESTAMP = "2022-01-20T23:19:52Z";

    @Test
    public void testDateParseAndFormat() {
        OffsetDateTime parsed = Fabric8IOUtil.parseTimestamp(TIMESTAMP);
        assertThat(parsed.toInstant().toEpochMilli()).isGreaterThan(0);

        // Now format it back
        String formatted = Fabric8IOUtil.formatTimestamp(parsed);
        assertThat(formatted).isEqualTo(TIMESTAMP);
    }
}