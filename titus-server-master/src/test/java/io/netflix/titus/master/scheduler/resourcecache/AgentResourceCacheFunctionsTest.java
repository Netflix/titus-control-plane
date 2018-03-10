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

package io.netflix.titus.master.scheduler.resourcecache;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import io.netflix.titus.common.util.time.Clocks;
import io.netflix.titus.common.util.time.TestClock;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class AgentResourceCacheFunctionsTest {

    @Test
    public void testCreateNewNetworkInterface() {
        TestClock clock = Clocks.test();
        long timestamp = clock.wallTime();
        AgentResourceCacheNetworkInterface networkInterface = AgentResourceCacheFunctions.createNetworkInterface(1,
                Collections.singletonMap("192.168.1.1", Collections.singleton("sg-1234")), Collections.emptySet(),
                false, timestamp);

        Assertions.assertThat(networkInterface).isNotNull();
    }

    @Test
    public void testUpdateNetworkInterfaceWithSameSecurityGroups() {
        TestClock clock = Clocks.test();
        long originalTimestamp = clock.wallTime();
        AgentResourceCacheNetworkInterface original = AgentResourceCacheFunctions.createNetworkInterface(1,
                Collections.singletonMap("192.168.1.1", Collections.singleton("task1")), Collections.singleton("sg-1234"),
                false, originalTimestamp);
        clock.advanceTime(1, TimeUnit.MINUTES);
        long updatedTimestamp = clock.wallTime();
        AgentResourceCacheNetworkInterface updated = AgentResourceCacheFunctions.createNetworkInterface(1,
                Collections.singletonMap("192.168.1.2", Collections.singleton("task2")), Collections.singleton("sg-1234"),
                false, updatedTimestamp);
        AgentResourceCacheNetworkInterface merged = AgentResourceCacheFunctions.updateNetworkInterface(original, updated);
        Assertions.assertThat(merged.getSecurityGroupIds()).containsExactly("sg-1234");
        Assertions.assertThat(merged.getIpAddresses()).containsOnlyKeys("192.168.1.1", "192.168.1.2");
        Assertions.assertThat(merged.getTimestamp()).isEqualTo(updatedTimestamp);
    }

    @Test
    public void testUpdateNetworkInterfaceWithDifferentSecurityGroups() {
        TestClock clock = Clocks.test();
        long originalTimestamp = clock.wallTime();
        AgentResourceCacheNetworkInterface original = AgentResourceCacheFunctions.createNetworkInterface(1,
                Collections.singletonMap("192.168.1.1", Collections.singleton("task1")), Collections.singleton("sg-1234"),
                false, originalTimestamp);
        clock.advanceTime(1, TimeUnit.MINUTES);
        long updatedTimestamp = clock.wallTime();
        AgentResourceCacheNetworkInterface updated = AgentResourceCacheFunctions.createNetworkInterface(1,
                Collections.singletonMap("192.168.1.2", Collections.singleton("task2")), Collections.singleton("sg-4321"),
                false, updatedTimestamp);
        AgentResourceCacheNetworkInterface merged = AgentResourceCacheFunctions.updateNetworkInterface(original, updated);
        Assertions.assertThat(merged.getSecurityGroupIds()).containsExactly("sg-4321");
        Assertions.assertThat(merged.getIpAddresses()).containsOnlyKeys("192.168.1.2");
        Assertions.assertThat(merged.getTimestamp()).isEqualTo(updatedTimestamp);
    }

    @Test
    public void testUpdateNetworkInterfaceByRemovingTask() {
        TestClock clock = Clocks.test();
        long originalTimestamp = clock.wallTime();
        AgentResourceCacheNetworkInterface original = AgentResourceCacheFunctions.createNetworkInterface(1,
                Collections.singletonMap("192.168.1.1", Collections.singleton("task1")), Collections.singleton("sg-1234"),
                false, originalTimestamp);
        clock.advanceTime(1, TimeUnit.MINUTES);
        long updatedTimestamp = clock.wallTime();
        AgentResourceCacheNetworkInterface updated = AgentResourceCacheFunctions.removeTaskIdFromNetworkInterface("task1",
                "192.168.1.1", original, updatedTimestamp);
        Assertions.assertThat(updated.getSecurityGroupIds()).containsExactly("sg-1234");
        Assertions.assertThat(updated.getIpAddresses()).containsOnlyKeys("192.168.1.1");
        Assertions.assertThat(updated.getIpAddresses()).containsValues(Collections.emptySet());
        Assertions.assertThat(updated.getTimestamp()).isEqualTo(updatedTimestamp);
    }
}