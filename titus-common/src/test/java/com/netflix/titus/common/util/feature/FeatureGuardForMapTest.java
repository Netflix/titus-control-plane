/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.common.util.feature;

import java.util.Map;
import java.util.function.Function;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.feature.FeatureGuard.FeatureGuardResult;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FeatureGuardForMapTest {

    private static final Map<String, String> ATTRIBUTES = CollectionsExt.asMap(
            "key1", "value1",
            "key2", "value2",
            "keyWithEmptyValue", "",
            "keyWithNullValue", null
    );

    @Test
    public void testKeyValueMatcherWhiteList() {
        assertThat(testWhiteListMatcher("key1=value1")).isEqualTo(FeatureGuardResult.Approved);
        assertThat(testWhiteListMatcher("key1=.*")).isEqualTo(FeatureGuardResult.Approved);
        assertThat(testWhiteListMatcher("key2=.*")).isEqualTo(FeatureGuardResult.Approved);
        assertThat(testWhiteListMatcher("key(1|2)=value.*")).isEqualTo(FeatureGuardResult.Approved);
        assertThat(testWhiteListMatcher("keyWithEmptyValue=")).isEqualTo(FeatureGuardResult.Approved);
        assertThat(testWhiteListMatcher("keyWithNullValue=")).isEqualTo(FeatureGuardResult.Approved);
    }

    @Test
    public void testKeyValueMatcherBlackList() {
        assertThat(testBlackListMatcher("key1=value1")).isEqualTo(FeatureGuardResult.Denied);
        assertThat(testBlackListMatcher("keyWithEmptyValue=")).isEqualTo(FeatureGuardResult.Denied);
        assertThat(testBlackListMatcher("keyWithNullValue=")).isEqualTo(FeatureGuardResult.Denied);
    }

    private FeatureGuardResult testWhiteListMatcher(String whiteList) {
        return FeatureGuards.fromMap(Function.identity(), FeatureGuards.newWhiteList()
                .withTurnOnPredicate(() -> true)
                .withWhiteListRegExpSupplier("jobDescriptor.attributes", () -> whiteList)
                .withBlackListRegExpSupplier("jobDescriptor.attributes", () -> "NONE")
                .build()
        ).matches(ATTRIBUTES);
    }

    private FeatureGuardResult testBlackListMatcher(String blackList) {
        return FeatureGuards.fromMap(Function.identity(), FeatureGuards.newWhiteList()
                .withTurnOnPredicate(() -> true)
                .withWhiteListRegExpSupplier("jobDescriptor.attributes", () -> "NONE")
                .withBlackListRegExpSupplier("jobDescriptor.attributes", () -> blackList)
                .build()
        ).matches(ATTRIBUTES);
    }
}