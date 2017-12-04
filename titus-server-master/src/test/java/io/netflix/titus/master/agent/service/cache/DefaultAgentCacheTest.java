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

package io.netflix.titus.master.agent.service.cache;

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
import io.netflix.titus.testkit.stub.connector.cloud.InstanceGenerators;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.common.util.ExceptionExt.doCatch;
import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.FULL_CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.expectInstanceGroupUpdateEvent;
import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.expectInstanceUpdateEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultAgentCacheTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final Registry registry = new DefaultRegistry();

    private final AgentManagementConfiguration configuration = InstanceTestUtils.mockedAgentManagementConfiguration();

    private final AgentStore agentStore = mock(AgentStore.class);

    private final TestableInstanceCloudConnector testConnector = new TestableInstanceCloudConnector();

    private DataGenerator<InstanceGroup> instanceGroupsGenerator = InstanceGenerators.instanceGroups(5);
    private DataGenerator<Instance> instanceGenerator1;
    private DataGenerator<Instance> instanceGenerator2;

    private DefaultAgentCache cache;
    private ExtTestSubscriber<CacheUpdateEvent> eventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        instanceGroupsGenerator = instanceGroupsGenerator.apply(testConnector::addInstanceGroup, 2);
        instanceGenerator1 = InstanceGenerators.instances(testConnector.takeInstanceGroup(0)).apply(testConnector::addInstance, 5);
        instanceGenerator2 = InstanceGenerators.instances(testConnector.takeInstanceGroup(1)).apply(testConnector::addInstance, 5);

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
    public void testDiscoverNewInstanceGroup() throws Exception {
        int initialCount = cache.getInstanceGroups().size();
        instanceGroupsGenerator = instanceGroupsGenerator.apply(testConnector::addInstanceGroup);

        testScheduler.advanceTimeBy(FULL_CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getInstanceGroups()).hasSize(initialCount + 1);

        // We need to count both the setup additions, and this one
        verify(agentStore, times(initialCount + 1)).storeAgentInstanceGroup(any());

        expectInstanceGroupUpdateEvent(eventSubscriber, testConnector.takeInstanceGroup(2).getId());
    }

    @Test
    public void testInstanceGroupCloudUpdate() throws Exception {
        InstanceGroup updated = testConnector.takeInstanceGroup(0).toBuilder().withMax(100).build();
        testConnector.addInstanceGroup(updated);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        expectInstanceGroupUpdateEvent(eventSubscriber, testConnector.takeInstanceGroup(0).getId());
    }

    @Test
    public void testInstanceGroupStoreUpdate() throws Exception {
        testInstanceGroupUpdate(false);
    }

    @Test
    public void testInstanceGroupStoreUpdateAndCloudSync() throws Exception {
        testInstanceGroupUpdate(true);
    }

    private void testInstanceGroupUpdate(boolean withCloudSync) {
        int initialCount = cache.getInstanceGroups().size();

        AgentInstanceGroup updatedInstanceGroup = cache.getInstanceGroups().get(0).toBuilder()
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder().withState(InstanceGroupLifecycleState.Removable).build()
                ).build();
        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        if (withCloudSync) {
            cache.updateInstanceGroupStoreAndSyncCloud(updatedInstanceGroup).toObservable().subscribe(testSubscriber);
        } else {
            cache.updateInstanceGroupStore(updatedInstanceGroup).toObservable().subscribe(testSubscriber);
        }

        testScheduler.triggerActions();

        assertThat(cache.getInstanceGroup(updatedInstanceGroup.getId()).getLifecycleStatus().getState()).isEqualTo(InstanceGroupLifecycleState.Removable);

        // We need to count both the setup additions, and this one
        verify(agentStore, times(initialCount + 1)).storeAgentInstanceGroup(any());

        expectInstanceGroupUpdateEvent(eventSubscriber, updatedInstanceGroup.getId());
    }

    @Test
    public void testCleanupOfRemovedInstanceGroups() throws Exception {
        AgentInstanceGroup instanceGroup = cache.getInstanceGroups().get(0);
        testConnector.removeInstanceGroup(instanceGroup.getId());

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(doCatch(() -> cache.getInstanceGroup(instanceGroup.getId()))).isNotEmpty();
        expectInstanceGroupUpdateEvent(eventSubscriber, instanceGroup.getId());
    }

    @Test
    public void testDiscoverNewAgentInstance() throws Exception {
        String instanceGroupId = testConnector.takeInstanceGroup(0).getId();
        int initialCount = cache.getAgentInstances(instanceGroupId).size();

        instanceGenerator1 = instanceGenerator1.apply(testConnector::addInstance);
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getAgentInstances(instanceGroupId).size()).isEqualTo(initialCount + 1);
        expectInstanceGroupUpdateEvent(eventSubscriber, instanceGroupId);
    }

    @Test
    public void testAgentInstanceCloudUpdate() throws Exception {
        Instance updatedInstance = testConnector.takeInstance(0, 0).toBuilder()
                .withInstanceState(Instance.InstanceState.Terminated)
                .build();
        testConnector.addInstance(updatedInstance);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getAgentInstance(updatedInstance.getId()).getLifecycleStatus().getState()).isEqualTo(InstanceLifecycleState.Stopped);
        expectInstanceGroupUpdateEvent(eventSubscriber, testConnector.takeInstanceGroup(0).getId());
    }

    @Test
    public void testAgentInstanceConfigurationUpdate() throws Exception {
        String instanceId = testConnector.takeInstance(0, 0).getId();

        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        cache.updateAgentInstanceStore(cache.getAgentInstance(instanceId)).toObservable().subscribe(testSubscriber);

        testScheduler.triggerActions();

        verify(agentStore, times(1)).storeAgentInstance(any());
        expectInstanceUpdateEvent(eventSubscriber, instanceId);
    }

    @Test
    public void testCleanupOfRemovedAgentInstance() throws Exception {
        String instanceGroupId = testConnector.takeInstanceGroup(0).getId();
        int initialCount = cache.getAgentInstances(instanceGroupId).size();

        testConnector.removeInstance(testConnector.takeInstance(0, 0).getId());
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getAgentInstances(instanceGroupId).size()).isEqualTo(initialCount - 1);
        expectInstanceGroupUpdateEvent(eventSubscriber, instanceGroupId);
    }
}