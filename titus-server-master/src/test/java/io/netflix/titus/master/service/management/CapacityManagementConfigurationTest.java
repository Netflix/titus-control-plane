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

package io.netflix.titus.master.service.management;

import java.util.Map;

import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.api.PropertyFactory;
import com.netflix.archaius.config.MapConfig;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static io.netflix.titus.common.util.CollectionsExt.asMap;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * As the configuration consists of list of nested objects, verify that archaius2 can deal with this properly.
 */
public class CapacityManagementConfigurationTest {

    @Test
    public void testTierConfiguration() throws Exception {
        CapacityManagementConfiguration config = getCapacityManagementConfiguration(asMap(
                "titus.master.capacityManagement.tiers.0.instanceTypes", "m4.2xlarge",
                "titus.master.capacityManagement.tiers.0.buffer", "0.1"
        ));

        Map<String, CapacityManagementConfiguration.TierConfig> tiers = config.getTiers();
        Assertions.assertThat(tiers).isNotNull();

        CapacityManagementConfiguration.TierConfig tierConfig0 = tiers.get("0");
        assertThat(tierConfig0).isNotNull();

        assertThat(tierConfig0.getBuffer()).isEqualTo(0.1);
    }

    private CapacityManagementConfiguration getCapacityManagementConfiguration(Map<String, String> properties) {
        MapConfig config = MapConfig.from(properties);
        PropertyFactory factory = new DefaultPropertyFactory(config);
        ConfigProxyFactory configProxyFactory = new ConfigProxyFactory(config, config.getDecoder(), factory);
        return configProxyFactory.newProxy(CapacityManagementConfiguration.class);
    }
}