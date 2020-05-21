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

package com.netflix.titus.common.util.limiter.tokenbucket.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.netflix.archaius.api.config.SettableConfig;
import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.limiter.tokenbucket.FixedIntervalTokenBucketConfiguration;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FixedIntervalTokenBucketSupplierTest {

    private final SettableConfig settableConfig = new DefaultSettableConfig();

    private final FixedIntervalTokenBucketConfiguration configuration = Archaius2Ext.newConfiguration(
            FixedIntervalTokenBucketConfiguration.class, "junit", settableConfig
    );

    private final List<TokenBucket> buckets = new ArrayList<>();

    private final FixedIntervalTokenBucketSupplier supplier = new FixedIntervalTokenBucketSupplier(
            "junit",
            configuration,
            buckets::add,
            Optional.of(TitusRuntimes.internal())
    );

    @Test
    public void testTokenBucketPersistsIfNotReconfigured() {
        settableConfig.setProperty("junit.initialNumberOfTokens", "1");
        settableConfig.setProperty("junit.capacity", "1");
        settableConfig.setProperty("junit.numberOfTokensPerInterval", "0");
        TokenBucket tokenBucket = supplier.get();

        assertThat(tokenBucket.tryTake()).isTrue();
        assertThat(tokenBucket.tryTake()).isFalse();
        tokenBucket.refill(1);
        assertThat(tokenBucket.tryTake()).isTrue();

        assertThat(supplier.get() == tokenBucket).isTrue();

        assertThat(buckets).hasSize(2);
        assertThat(buckets.get(1)).isEqualTo(tokenBucket);
    }

    @Test
    public void testTokenBucketIsRecreatedIfConfigurationChanges() {
        settableConfig.setProperty("junit.initialNumberOfTokens", "1");
        settableConfig.setProperty("junit.capacity", "1");
        settableConfig.setProperty("junit.numberOfTokensPerInterval", "0");
        TokenBucket first = supplier.get();

        assertThat(first.tryTake()).isTrue();
        assertThat(first.tryTake()).isFalse();

        settableConfig.setProperty("junit.initialNumberOfTokens", "2");
        settableConfig.setProperty("junit.capacity", "2");
        TokenBucket second = supplier.get();
        assertThat(first != second).isTrue();

        assertThat(second.tryTake()).isTrue();
        assertThat(second.tryTake()).isTrue();
        assertThat(second.tryTake()).isFalse();

        assertThat(buckets).hasSize(3);
        assertThat(buckets.get(1)).isEqualTo(first);
        assertThat(buckets.get(2)).isEqualTo(second);
    }

    @Test
    public void testToString() {
        String text = supplier.get().toString();
        assertThat(text).isEqualTo("DefaultTokenBucket{name='junit', capacity=1, refillStrategy=FixedIntervalRefillStrategy{refillRate=1.00 refill/s}, numberOfTokens=0}");
    }
}