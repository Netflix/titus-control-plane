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

package io.netflix.titus.master.agent.service.vm;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.master.agent.service.AgentManagementConfiguration;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import io.netflix.titus.testkit.stub.connector.cloud.TestableInstanceCloudConnector;
import io.netflix.titus.testkit.stub.connector.cloud.VmGenerators;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.master.agent.service.vm.VmTestUtils.CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.vm.VmTestUtils.FULL_CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.vm.VmTestUtils.expectServerGroupUpdateEvent;
import static io.netflix.titus.master.agent.service.vm.VmTestUtils.mockedAgentManagementConfiguration;
import static org.assertj.core.api.Assertions.assertThat;

public class VmServersCacheTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final AgentManagementConfiguration configuration = mockedAgentManagementConfiguration();

    private final TestableInstanceCloudConnector testConnector = new TestableInstanceCloudConnector();

    private DataGenerator<InstanceGroup> serverGroupsGenerator = VmGenerators.vmServerGroups(5);
    private DataGenerator<Instance> instanceGenerator1;
    private DataGenerator<Instance> instanceGenerator2;

    private VmServersCache cache;
    private ExtTestSubscriber<CacheUpdateEvent> eventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        serverGroupsGenerator = serverGroupsGenerator.apply(testConnector::addVmServerGroup, 2);
        instanceGenerator1 = VmGenerators.vmServers(testConnector.takeServerGroup(0)).apply(testConnector::addVmServer, 5);
        instanceGenerator2 = VmGenerators.vmServers(testConnector.takeServerGroup(1)).apply(testConnector::addVmServer, 5);

        cache = VmServersCache.newInstance(configuration, testConnector, Collections.emptySet(), testScheduler);
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS); // This will populate the cache with the initial connector state
        cache.events().subscribe(eventSubscriber);
    }

    @Test
    public void testBootstrapWithKnownServerGroups() throws Exception {
        assertThat(serverGroupIds(cache.getServerGroups())).containsAll(testConnector.takeServerGroupIds());
        assertThat(cache.getServerGroups().get(0).getInstanceIds()).containsAll(serverIds(testConnector.takeServers(0)));
        assertThat(cache.getServerGroups().get(1).getInstanceIds()).containsAll(serverIds(testConnector.takeServers(1)));

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(eventSubscriber.takeNext()).isNull();
    }

    @Test
    public void testServerGroupAdded() throws Exception {
        int initialCount = cache.getServerGroups().size();
        serverGroupsGenerator = serverGroupsGenerator.apply(testConnector::addVmServerGroup);

        testScheduler.advanceTimeBy(FULL_CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getServerGroups()).hasSize(initialCount + 1);

        CacheUpdateEvent event = eventSubscriber.takeNext();
        assertThat(event).isNotNull();
        assertThat(event.getType()).isEqualTo(CacheUpdateType.Refreshed);
    }

    @Test
    public void testServerGroupChanged() throws Exception {
        InstanceGroup updated = testConnector.takeServerGroup(0).toBuilder().withMax(100).build();
        testConnector.addVmServerGroup(updated);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(testConnector.takeServerGroup(0)).isEqualTo(updated);

        expectServerGroupUpdateEvent(eventSubscriber, updated.getId());
    }

    @Test
    public void testServerGroupRemoved() throws Exception {
        int initialCount = cache.getServerGroups().size();

        String removedServerGroupId = testConnector.takeServerGroup(0).getId();
        testConnector.removeVmServerGroup(removedServerGroupId);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getServerGroups()).hasSize(initialCount - 1);
        expectServerGroupUpdateEvent(eventSubscriber, removedServerGroupId);
    }

    @Test
    public void testInstanceAdded() throws Exception {
        String serverGroupId = testConnector.takeServerGroup(0).getId();
        int initialCount = cache.getServerGroup(serverGroupId).getInstanceIds().size();

        instanceGenerator1 = instanceGenerator1.apply(testConnector::addVmServer);
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getServerGroup(serverGroupId).getInstanceIds().size()).isEqualTo(initialCount + 1);
        expectServerGroupUpdateEvent(eventSubscriber, serverGroupId);
    }

    @Test
    public void testInstanceChanged() throws Exception {
        Instance updatedServer = testConnector.takeServer(0, 0).toBuilder()
                .withInstanceState(Instance.InstanceState.Terminated)
                .build();
        testConnector.addVmServer(updatedServer);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getAgentInstance(updatedServer.getId()).getInstanceState()).isEqualTo(Instance.InstanceState.Terminated);
        expectServerGroupUpdateEvent(eventSubscriber, testConnector.takeServerGroup(0).getId());
    }

    @Test
    public void testInstanceRemoved() throws Exception {
        String serverGroupId = testConnector.takeServerGroup(0).getId();
        int initialCount = cache.getServerGroup(serverGroupId).getInstanceIds().size();

        testConnector.removeVmServer(testConnector.takeServer(0, 0).getId());
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getServerGroup(serverGroupId).getInstanceIds().size()).isEqualTo(initialCount - 1);
        expectServerGroupUpdateEvent(eventSubscriber, serverGroupId);
    }

    private static List<String> serverGroupIds(Collection<InstanceGroup> instanceGroups) {
        return instanceGroups.stream().map(InstanceGroup::getId).collect(Collectors.toList());
    }

    private static List<String> serverIds(Collection<Instance> instances) {
        return instances.stream().map(Instance::getId).collect(Collectors.toList());
    }
}