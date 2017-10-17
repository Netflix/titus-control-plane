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

import io.netflix.titus.common.aws.AwsInstanceType;
import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolver;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AwsServerInfoResolverTest {

    @Test
    public void testInstanceTypeMapping() throws Exception {
        ServerInfoResolver resolver = ServerInfoResolvers.fromAwsInstanceTypes();

        Assertions.assertThat(resolver.resolveAll()).hasSize(AwsInstanceType.values().length * 2);

        ServerInfo m4_4xlarge = resolver.resolve("m4.4xlarge").get();
        assertThat(m4_4xlarge.getCpus()).isEqualTo(16);
        assertThat(m4_4xlarge.getGpus()).isEqualTo(0);
        assertThat(m4_4xlarge.getMemoryGB()).isEqualTo(64);
        assertThat(m4_4xlarge.getStorageGB()).isEqualTo(512);
        assertThat(m4_4xlarge.getNetworkMbs()).isEqualTo(2000);
    }
}