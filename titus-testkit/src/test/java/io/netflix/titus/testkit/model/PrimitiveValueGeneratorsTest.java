/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.model;

import org.junit.Test;

import static io.netflix.titus.testkit.model.PrimitiveValueGenerators.HEX_DIGITS_SET;
import static io.netflix.titus.testkit.model.PrimitiveValueGenerators.hexValues;
import static io.netflix.titus.testkit.model.PrimitiveValueGenerators.ipv4CIDRs;
import static org.assertj.core.api.Assertions.assertThat;

public class PrimitiveValueGeneratorsTest {

    @Test
    public void testIpv4Cdirs() throws Exception {
        assertThat(ipv4CIDRs("10.0.0.0/24").limit(2).toList()).contains("10.0.0.1", "10.0.0.2");
    }

    @Test
    public void testInternetDomains() throws Exception {
        assertThat(PrimitiveValueGenerators.internetDomains().getValue()).isEqualTo("serviceA.com");
    }

    @Test
    public void testEmailAddresses() throws Exception {
        assertThat(PrimitiveValueGenerators.emailAddresses().getValue()).isEqualTo("john@serviceA.com");
    }

    @Test
    public void testSemanticVersion() throws Exception {
        assertThat(PrimitiveValueGenerators.semanticVersions(3).getValue()).isEqualTo("1.0.0");
    }

    @Test
    public void testHexValues() throws Exception {
        char[] hexValue = hexValues(16).toList(1).get(0).toCharArray();
        for (char c : hexValue) {
            assertThat(HEX_DIGITS_SET.contains(c)).isTrue();
        }
    }
}