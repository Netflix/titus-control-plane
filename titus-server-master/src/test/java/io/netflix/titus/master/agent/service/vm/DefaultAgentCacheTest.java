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

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import io.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import io.netflix.titus.api.agent.model.InstanceLifecycleState;
import io.netflix.titus.api.agent.store.AgentStore;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.master.agent.service.AgentManagementConfiguration;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import io.netflix.titus.testkit.stub.connector.cloud.TestableInstanceCloudConnector;
import io.netflix.titus.testkit.stub.connector.cloud.VmGenerators;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.common.util.ExceptionExt.doCatch;
import static io.netflix.titus.master.agent.service.vm.VmTestUtils.CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.vm.VmTestUtils.FULL_CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.vm.VmTestUtils.expectServerGroupUpdateEvent;
import static io.netflix.titus.master.agent.service.vm.VmTestUtils.expectServerUpdateEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultAgentCacheTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final Registry registry = new DefaultRegistry();

    private final AgentManagementConfiguration configuration = VmTestUtils.mockedAgentManagementConfiguration();

    private final AgentStore agentStore = mock(AgentStore.class);

    private final TestableInstanceCloudConnector testConnector = new TestableInstanceCloudConnector();

    private DataGenerator<InstanceGroup> serverGroupsGenerator = VmGenerators.vmServerGroups(5);
    private DataGenerator<Instance> instanceGenerator1;
    private DataGenerator<Instance> instanceGenerator2;

    private DefaultAgentCache cache;
    private ExtTestSubscriber<CacheUpdateEvent> eventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        serverGroupsGenerator = serverGroupsGenerator.apply(testConnector::addVmServerGroup, 2);
        instanceGenerator1 = VmGenerators.vmServers(testConnector.takeServerGroup(0)).apply(testConnector::addVmServer, 5);
        instanceGenerator2 = VmGenerators.vmServers(testConnector.takeServerGroup(1)).apply(testConnector::addVmServer, 5);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS); // This will populate the cache with the initial connector state

        when(agentStore.retrieveAgentInstanceGroups()).thenReturn(Observable.empty());
        when(agentStore.retrieveAgentInstances()).thenReturn(Observable.empty());
        when(agentStore.storeAgentInstanceGroup(any())).thenReturn(Completable.complete());
        when(agentStore.storeAgentInstance(any())).thenReturn(Completable.complete());

        cache = new DefaultAgentCache(configuration, agentStore, testConnector, registry, testScheduler);
        cache.enterActiveMode();
        testScheduler.triggerActions();

        cache.events().subscribe(eventSubscriber);
    }

    @Test
    public void testDiscoverNewServerGroup() throws Exception {
        int initialCount = cache.getInstanceGroups().size();
        serverGroupsGenerator = serverGroupsGenerator.apply(testConnector::addVmServerGroup);

        testScheduler.advanceTimeBy(FULL_CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getInstanceGroups()).hasSize(initialCount + 1);

        // We need to count both the setup additions, and this one
        verify(agentStore, times(initialCount + 1)).storeAgentInstanceGroup(any());

        expectServerGroupUpdateEvent(eventSubscriber, testConnector.takeServerGroup(2).getId());
    }

    @Test
    public void testServerGroupCloudUpdate() throws Exception {
        InstanceGroup updated = testConnector.takeServerGroup(0).toBuilder().withMax(100).build();
        testConnector.addVmServerGroup(updated);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        expectServerGroupUpdateEvent(eventSubscriber, testConnector.takeServerGroup(0).getId());
    }

    @Test
    public void testServerGroupStoreUpdate() throws Exception {
        testServerGroupUpdate(false);
    }

    @Test
    public void testServerGroupStoreUpdateAndCloudSync() throws Exception {
        testServerGroupUpdate(true);
    }

    private void testServerGroupUpdate(boolean withCloudSync) {
        int initialCount = cache.getInstanceGroups().size();

        AgentInstanceGroup updatedServerGroup = cache.getInstanceGroups().get(0).toBuilder()
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder().withState(InstanceGroupLifecycleState.Removable).build()
                ).build();
        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        if (withCloudSync) {
            cache.updateInstanceGroupStoreAndSyncCloud(updatedServerGroup).toObservable().subscribe(testSubscriber);
        } else {
            cache.updateInstanceGroupStore(updatedServerGroup).toObservable().subscribe(testSubscriber);
        }

        testScheduler.triggerActions();

        assertThat(cache.getInstanceGroup(updatedServerGroup.getId()).getLifecycleStatus().getState()).isEqualTo(InstanceGroupLifecycleState.Removable);

        // We need to count both the setup additions, and this one
        verify(agentStore, times(initialCount + 1)).storeAgentInstanceGroup(any());

        expectServerGroupUpdateEvent(eventSubscriber, testConnector.takeServerGroup(0).getId());
    }

    @Test
    public void testCleanupOfRemovedServerGroups() throws Exception {
        AgentInstanceGroup serverGroup = cache.getInstanceGroups().get(0);
        testConnector.removeVmServerGroup(serverGroup.getId());

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(doCatch(() -> cache.getInstanceGroup(serverGroup.getId()))).isNotEmpty();
        expectServerGroupUpdateEvent(eventSubscriber, serverGroup.getId());
    }

    @Test
    public void testDiscoverNewAgentServer() throws Exception {
        String serverGroupId = testConnector.takeServerGroup(0).getId();
        int initialCount = cache.getAgentInstances(serverGroupId).size();

        instanceGenerator1 = instanceGenerator1.apply(testConnector::addVmServer);
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getAgentInstances(serverGroupId).size()).isEqualTo(initialCount + 1);
        expectServerGroupUpdateEvent(eventSubscriber, serverGroupId);
    }

    @Test
    public void testAgentServerCloudUpdate() throws Exception {
        Instance updatedServer = testConnector.takeServer(0, 0).toBuilder()
                .withInstanceState(Instance.InstanceState.Terminated)
                .build();
        testConnector.addVmServer(updatedServer);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getAgentInstance(updatedServer.getId()).getLifecycleStatus().getState()).isEqualTo(InstanceLifecycleState.Stopped);
        expectServerGroupUpdateEvent(eventSubscriber, testConnector.takeServerGroup(0).getId());
    }

    @Test
    public void testAgentServerConfigurationUpdate() throws Exception {
        String serverId = testConnector.takeServer(0, 0).getId();

        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        cache.updateAgentInstanceStore(cache.getAgentInstance(serverId)).toObservable().subscribe(testSubscriber);

        testScheduler.triggerActions();

        verify(agentStore, times(1)).storeAgentInstance(any());
        expectServerUpdateEvent(eventSubscriber, serverId);
    }

    @Test
    public void testCleanupOfRemovedAgentServer() throws Exception {
        String serverGroupId = testConnector.takeServerGroup(0).getId();
        int initialCount = cache.getAgentInstances(serverGroupId).size();

        testConnector.removeVmServer(testConnector.takeServer(0, 0).getId());
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getAgentInstances(serverGroupId).size()).isEqualTo(initialCount - 1);
        expectServerGroupUpdateEvent(eventSubscriber, serverGroupId);
    }
}