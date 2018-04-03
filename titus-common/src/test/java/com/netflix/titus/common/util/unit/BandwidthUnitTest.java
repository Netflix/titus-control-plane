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

import static com.netflix.titus.common.util.unit.BandwidthUnit._1G;
import static com.netflix.titus.common.util.unit.BandwidthUnit._1K;
import static com.netflix.titus.common.util.unit.BandwidthUnit._1M;
import static org.assertj.core.api.Assertions.assertThat;

public class BandwidthUnitTest {

    @Test
    public void testParsing() throws Exception {
        assertThat(BandwidthUnit.bpsOf("100b")).isEqualTo(100);
        assertThat(BandwidthUnit.bpsOf(" 100 BPS ")).isEqualTo(100);
        assertThat(BandwidthUnit.bpsOf("100k")).isEqualTo(100 * _1K);
        assertThat(BandwidthUnit.bpsOf(" 100 KBps ")).isEqualTo(100 * _1K);
        assertThat(BandwidthUnit.bpsOf("100m")).isEqualTo(100 * _1M);
        assertThat(BandwidthUnit.bpsOf(" 100 MBps ")).isEqualTo(100 * _1M);
        assertThat(BandwidthUnit.bpsOf("100g")).isEqualTo(100 * _1G);
        assertThat(BandwidthUnit.bpsOf(" 100 GBps ")).isEqualTo(100 * _1G);
    }
}