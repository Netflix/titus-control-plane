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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MemoryUnitTest {

    @Test
    public void testParsing() throws Exception {
        assertThat(MemoryUnit.bytesOf("100b")).isEqualTo(100);
        assertThat(MemoryUnit.bytesOf(" 100 BYTES ")).isEqualTo(100);
        assertThat(MemoryUnit.bytesOf("100k")).isEqualTo(100 * MemoryUnit._1KB);
        assertThat(MemoryUnit.bytesOf(" 100 KB ")).isEqualTo(100 * MemoryUnit._1KB);
        assertThat(MemoryUnit.bytesOf("100m")).isEqualTo(100 * MemoryUnit._1MB);
        assertThat(MemoryUnit.bytesOf(" 100 MB ")).isEqualTo(100 * MemoryUnit._1MB);
        assertThat(MemoryUnit.bytesOf("100g")).isEqualTo(100 * MemoryUnit._1GB);
        assertThat(MemoryUnit.bytesOf(" 100 GB ")).isEqualTo(100 * MemoryUnit._1GB);
    }
}