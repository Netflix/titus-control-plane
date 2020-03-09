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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static com.netflix.titus.common.util.StringExt.parseEnumListIgnoreCase;
import static com.netflix.titus.common.util.StringExt.parseKeyValueList;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StringExtTest {

    private enum EnumValue {A, B, C, D}

    @Test
    public void testParseEnumListIgnoreCase() {
        assertThat(parseEnumListIgnoreCase("", EnumValue.class)).isEmpty();
        assertThat(parseEnumListIgnoreCase("  ", EnumValue.class)).isEmpty();
        assertThat(parseEnumListIgnoreCase("a , B", EnumValue.class)).contains(EnumValue.A, EnumValue.B);

        Map<String, List<EnumValue>> grouping = singletonMap("ab", asList(EnumValue.C, EnumValue.D));
        assertThat(parseEnumListIgnoreCase("a , B, AB", EnumValue.class, grouping::get)).contains(EnumValue.A, EnumValue.B);

        try {
            parseEnumListIgnoreCase("a , bb", EnumValue.class);
            fail("Expected to fail during parsing invalid enum value");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).contains("bb");
        }
    }

    @Test
    public void testParseKeyValueList() {
        Map<String, String> actual = parseKeyValueList("key1:value1,key2:value2,key3:,key4");
        assertThat(actual).containsAllEntriesOf(ImmutableMap.<String, String>
                builder().put("key1", "value1").put("key2", "value2").put("key3", "").put("key4", "").build()
        );
    }

    @Test
    public void testRemoveQuotes() {
        assertThat(StringExt.removeSurroundingQuotes("\"abc")).isEqualTo("\"abc");
        assertThat(StringExt.removeSurroundingQuotes("\"abc\"")).isEqualTo("abc");
        assertThat(StringExt.removeSurroundingQuotes("abc\"")).isEqualTo("abc\"");
    }

    @Test
    public void testParseDurationList() {
        assertThat(StringExt.parseDurationMsList("abc")).isEmpty();

        assertThat(StringExt.parseDurationMsList("1, ,")).isEmpty();

        assertThat(StringExt.parseDurationMsList("1, 2  ").orElse(Collections.emptyList())).contains(
                Duration.ofMillis(1), Duration.ofMillis(2)
        );

        assertThat(StringExt.parseDurationMsList("1,2,3").orElse(Collections.emptyList())).contains(
                Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(3)
        );
    }

    @Test
    public void testGzipAndBase64Encode() {
        String input = "{aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:bbbbbbbbbbbbbbbbbbbbbbbbbbb}";
        String output = StringExt.gzipAndBase64Encode(input);
        assertThat(output.length()).isLessThan(input.length());
    }
}