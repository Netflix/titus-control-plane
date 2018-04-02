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

package com.netflix.titus.master.mesos;

import java.net.InetSocketAddress;

import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.mesos.resolver.DefaultMesosMasterResolver;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultMesosMasterResolverTest {

    private final MasterConfiguration config = mock(MasterConfiguration.class);

    @Test
    public void testWithZookeeperAddress() throws Exception {
        when(config.getMasterLocation()).thenReturn("zk://zookeeper_address/titus/mesos");
        DefaultMesosMasterResolver resolver = new DefaultMesosMasterResolver(config);

        assertThat(resolver.resolveCanonical()).contains("zk://zookeeper_address/titus/mesos");
        assertThat(resolver.resolveLeader()).isEmpty();
        assertThat(resolver.resolveMesosAddresses()).isEmpty();
    }

    @Test
    public void testWithHostPortAddressList() throws Exception {
        when(config.getMasterLocation()).thenReturn("serverA:5051,serverB");
        DefaultMesosMasterResolver resolver = new DefaultMesosMasterResolver(config);

        assertThat(resolver.resolveCanonical()).contains("serverA:5051,serverB");
        assertThat(resolver.resolveLeader()).isEmpty();
        assertThat(resolver.resolveMesosAddresses()).containsExactly(
                new InetSocketAddress("serverA", 5051),
                new InetSocketAddress("serverB", 5050)
        );
    }
}