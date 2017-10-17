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

package io.netflix.titus.master.agent.service.server.internal;

import java.util.Map;

import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class PropertyServerInfoResolverTest {

    @Test
    public void testPropertyBasedConfiguration() throws Exception {
        Map<String, String> properties = CollectionsExt.asMap(
                "m4.4xlarge.cpus", "16",
                "m4.4xlarge.memoryGB", "64",
                "m4.4xlarge.storageGB", "512",
                "m4.4xlarge.networkMbs", "2000",
                "m4.4xlarge.attributes.ebsBandwidthMbs", "2000"
        );
        ServerInfoResolver resolver = ServerInfoResolvers.fromProperties(properties, 2);
        Assertions.assertThat(resolver.resolveAll()).hasSize(1);
        Assertions.assertThat(resolver.resolve("m4.4xlarge")).isEqualTo(ServerInfoResolvers.fromAwsInstanceTypes().resolve("m4.4xlarge"));
    }
}