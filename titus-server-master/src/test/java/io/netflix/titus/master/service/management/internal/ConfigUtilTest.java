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

package io.netflix.titus.master.service.management.internal;

import java.util.Map;

import com.netflix.archaius.ConfigProxyFactory;
import com.netflix.archaius.DefaultPropertyFactory;
import com.netflix.archaius.config.MapConfig;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.service.management.CapacityManagementConfiguration;
import org.junit.Test;

import static io.netflix.titus.common.util.CollectionsExt.asMap;
import static org.assertj.core.api.Assertions.assertThat;

public class ConfigUtilTest {

    @Test
    public void testCompleteTierConfig() throws Exception {
        CapacityManagementConfiguration config = from(asMap(
                "titus.master.capacityManagement.tiers.0.instanceTypes", "m4.4xlarge",
                "titus.master.capacityManagement.tiers.0.buffer", "0.2",
                "titus.master.capacityManagement.tiers.1.instanceTypes", "r3.8xlarge",
                "titus.master.capacityManagement.tiers.1.buffer", "0.3"
        ));

        assertThat(ConfigUtil.getTierInstanceTypes(Tier.Critical, config)).containsOnly(AwsInstanceType.M4_4XLARGE_ID);
        assertThat(ConfigUtil.getTierBuffer(Tier.Critical, config)).isEqualTo(0.2);

        assertThat(ConfigUtil.getTierInstanceTypes(Tier.Flex, config)).containsOnly(AwsInstanceType.R3_8XLARGE_ID);
        assertThat(ConfigUtil.getTierBuffer(Tier.Flex, config)).isEqualTo(0.3);

        assertThat(ConfigUtil.getAllInstanceTypes(config)).containsOnly(AwsInstanceType.M4_4XLARGE_ID, AwsInstanceType.R3_8XLARGE_ID);
    }

    @Test
    public void testInstanceTypeConfig() throws Exception {
        CapacityManagementConfiguration config = from(asMap(
                "titus.master.capacityManagement.instanceTypes.0.name", "DEFAULT",
                "titus.master.capacityManagement.instanceTypes.0.minSize", "3",
                "titus.master.capacityManagement.instanceTypes.1.name", "g2.8xlarge",
                "titus.master.capacityManagement.instanceTypes.1.minSize", "5"
        ));

        assertThat(ConfigUtil.getInstanceTypeMinSize(config, AwsInstanceType.M4_4XLARGE_ID)).isEqualTo(3);
        assertThat(ConfigUtil.getInstanceTypeMinSize(config, AwsInstanceType.G2_8XLARGE_ID)).isEqualTo(5);
    }

    private CapacityManagementConfiguration from(Map<String, String> props) {
        MapConfig properties = new MapConfig(props);
        ConfigProxyFactory factory = new ConfigProxyFactory(properties, new DefaultPropertyFactory(properties));
        return factory.newProxy(CapacityManagementConfiguration.class);
    }
}