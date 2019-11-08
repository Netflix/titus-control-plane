/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.util.loadshedding.tokenbucket;

import java.util.HashMap;
import java.util.List;

import com.netflix.archaius.config.MapConfig;
import com.netflix.titus.common.util.CollectionsExt;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ArchaiusTokenBucketAdmissionConfigurationParserTest {

    @Test
    public void testNoConfiguration() {
        MapConfig config = new MapConfig(new HashMap<>());
        ArchaiusTokenBucketAdmissionConfigurationParser parser = new ArchaiusTokenBucketAdmissionConfigurationParser(config);
        assertThat(parser.parse()).isEmpty();
    }

    @Test
    public void testValidConfiguration() {
        MapConfig config = new MapConfig(CollectionsExt.merge(
                TokenBucketTestConfigurations.NOT_SHARED_PROPERTIES,
                TokenBucketTestConfigurations.SHARED_ANY_PROPERTIES
        ));
        ArchaiusTokenBucketAdmissionConfigurationParser parser = new ArchaiusTokenBucketAdmissionConfigurationParser(config);

        List<TokenBucketConfiguration> configuration = parser.parse();
        assertThat(configuration).hasSize(2);
        assertThat(configuration.get(0)).isEqualTo(TokenBucketTestConfigurations.NOT_SHARED_CONFIGURATION);
        assertThat(configuration.get(1)).isEqualTo(TokenBucketTestConfigurations.SHARED_ANY_CONFIGURATION);
    }

    @Test
    public void testPartiallyInvalid() {
        MapConfig config = new MapConfig(CollectionsExt.merge(
                TokenBucketTestConfigurations.NOT_SHARED_BAD_PROPERTIES,
                TokenBucketTestConfigurations.SHARED_ANY_PROPERTIES
        ));
        ArchaiusTokenBucketAdmissionConfigurationParser parser = new ArchaiusTokenBucketAdmissionConfigurationParser(config);

        List<TokenBucketConfiguration> configuration = parser.parse();
        assertThat(configuration).hasSize(1);
        assertThat(configuration.get(0).getName()).isEqualTo("default");
    }

    @Test
    public void testAllInvalid() {
        MapConfig config = new MapConfig(CollectionsExt.merge(
                TokenBucketTestConfigurations.NOT_SHARED_BAD_PROPERTIES,
                TokenBucketTestConfigurations.SHARED_ANY_BAD_PROPERTIES
        ));
        ArchaiusTokenBucketAdmissionConfigurationParser parser = new ArchaiusTokenBucketAdmissionConfigurationParser(config);
        assertThat(parser.parse()).isEmpty();
    }
}