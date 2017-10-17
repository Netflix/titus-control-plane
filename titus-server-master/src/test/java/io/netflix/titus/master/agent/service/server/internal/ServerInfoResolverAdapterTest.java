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

import io.netflix.titus.master.agent.ServerInfo;
import io.netflix.titus.master.agent.service.server.ServerInfoResolvers;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ServerInfoResolverAdapterTest {

    @Test
    public void testAdaptation() throws Exception {
        ServerInfo reference = ServerInfoResolvers.fromAwsInstanceTypes().resolve("m4.4xlarge").get();
        ServerInfo adapted = ServerInfoResolvers.adapt(
                ServerInfoResolvers.fromAwsInstanceTypes(),
                cpus -> cpus - 2,
                memoryGB -> memoryGB - 1,
                diskGB -> diskGB - 10
        ).resolve("m4.4xlarge").get();

        assertThat(adapted.getCpus()).isEqualTo(reference.getCpus() - 2);
        assertThat(adapted.getMemoryGB()).isEqualTo(reference.getMemoryGB() - 1);
        assertThat(adapted.getStorageGB()).isEqualTo(reference.getStorageGB() - 10);
    }
}