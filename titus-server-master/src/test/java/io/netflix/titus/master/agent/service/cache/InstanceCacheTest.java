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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.connector.cloud.Instance;
import io.netflix.titus.api.connector.cloud.InstanceGroup;
import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.master.agent.service.AgentManagementConfiguration;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import io.netflix.titus.testkit.stub.connector.cloud.InstanceGenerators;
import io.netflix.titus.testkit.stub.connector.cloud.TestableInstanceCloudConnector;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.FULL_CACHE_REFRESH_INTERVAL_MS;
import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.expectInstanceGroupUpdateEvent;
import static io.netflix.titus.master.agent.service.cache.InstanceTestUtils.mockedAgentManagementConfiguration;
import static org.assertj.core.api.Assertions.assertThat;

public class InstanceCacheTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final AgentManagementConfiguration configuration = mockedAgentManagementConfiguration();

    private final TestableInstanceCloudConnector testConnector = new TestableInstanceCloudConnector();

    private final Registry registry = new DefaultRegistry();

    private DataGenerator<InstanceGroup> instanceGroupsGenerator = InstanceGenerators.instanceGroups(5);
    private DataGenerator<Instance> instanceGenerator1;
    private DataGenerator<Instance> instanceGenerator2;

    private InstanceCache cache;
    private ExtTestSubscriber<CacheUpdateEvent> eventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        instanceGroupsGenerator = instanceGroupsGenerator.apply(testConnector::addInstanceGroup, 2);
        instanceGenerator1 = InstanceGenerators.instances(testConnector.takeInstanceGroup(0)).apply(testConnector::addInstance, 5);
        instanceGenerator2 = InstanceGenerators.instances(testConnector.takeInstanceGroup(1)).apply(testConnector::addInstance, 5);

        cache = InstanceCache.newInstance(configuration, testConnector, Collections.emptySet(), registry, testScheduler);
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS); // This will populate the cache with the initial connector state
        cache.events().subscribe(eventSubscriber);
    }

    @Test
    public void testBootstrapWithKnownInstanceGroups() throws Exception {
        assertThat(instanceGroupIds(cache.getInstanceGroups())).containsAll(testConnector.takeInstanceGroupIds());
        InstanceGroup firstInstanceGroup = testConnector.takeInstanceGroup(0);
        assertThat(cache.getInstanceGroups().contains(firstInstanceGroup));
        assertThat(firstInstanceGroup.getInstanceIds()).containsAll(instanceIds(testConnector.takeInstances(0)));

        InstanceGroup secondInstanceGroup = testConnector.takeInstanceGroup(1);
        assertThat(cache.getInstanceGroups().contains(secondInstanceGroup));
        assertThat(secondInstanceGroup.getInstanceIds()).containsAll(instanceIds(testConnector.takeInstances(1)));

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(eventSubscriber.takeNext()).isNull();
    }

    @Test
    public void testInstanceGroupAdded() throws Exception {
        int initialCount = cache.getInstanceGroups().size();
        instanceGroupsGenerator = instanceGroupsGenerator.apply(testConnector::addInstanceGroup);

        testScheduler.advanceTimeBy(FULL_CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getInstanceGroups()).hasSize(initialCount + 1);

        CacheUpdateEvent event = eventSubscriber.takeNext();
        assertThat(event).isNotNull();
        assertThat(event.getType()).isEqualTo(CacheUpdateType.Refreshed);
    }

    @Test
    public void testInstanceGroupChanged() throws Exception {
        InstanceGroup updated = testConnector.takeInstanceGroup(0).toBuilder().withMax(100).build();
        testConnector.addInstanceGroup(updated);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(testConnector.takeInstanceGroup(0)).isEqualTo(updated);

        expectInstanceGroupUpdateEvent(eventSubscriber, updated.getId());
    }

    @Test
    public void testInstanceGroupRemoved() throws Exception {
        int initialCount = cache.getInstanceGroups().size();

        String removedInstanceGroupId = testConnector.takeInstanceGroup(0).getId();
        testConnector.removeInstanceGroup(removedInstanceGroupId);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getInstanceGroups()).hasSize(initialCount - 1);
        expectInstanceGroupUpdateEvent(eventSubscriber, removedInstanceGroupId);
    }

    @Test
    public void testInstanceAdded() throws Exception {
        String instanceGroupId = testConnector.takeInstanceGroup(0).getId();
        int initialCount = cache.getInstanceGroup(instanceGroupId).getInstanceIds().size();

        instanceGenerator1 = instanceGenerator1.apply(testConnector::addInstance);
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getInstanceGroup(instanceGroupId).getInstanceIds().size()).isEqualTo(initialCount + 1);
        expectInstanceGroupUpdateEvent(eventSubscriber, instanceGroupId);
    }

    @Test
    public void testInstanceChanged() throws Exception {
        Instance updatedInstance = testConnector.takeInstance(0, 0).toBuilder()
                .withInstanceState(Instance.InstanceState.Terminated)
                .build();
        testConnector.addInstance(updatedInstance);

        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(cache.getAgentInstance(updatedInstance.getId()).getInstanceState()).isEqualTo(Instance.InstanceState.Terminated);
        expectInstanceGroupUpdateEvent(eventSubscriber, testConnector.takeInstanceGroup(0).getId());
    }

    @Test
    public void testInstanceRemoved() throws Exception {
        String instanceGroupId = testConnector.takeInstanceGroup(0).getId();
        int initialCount = cache.getInstanceGroup(instanceGroupId).getInstanceIds().size();

        testConnector.removeInstance(testConnector.takeInstance(0, 0).getId());
        testScheduler.advanceTimeBy(CACHE_REFRESH_INTERVAL_MS, TimeUnit.MILLISECONDS);

        assertThat(cache.getInstanceGroup(instanceGroupId).getInstanceIds().size()).isEqualTo(initialCount - 1);
        expectInstanceGroupUpdateEvent(eventSubscriber, instanceGroupId);
    }

    private static List<String> instanceGroupIds(Collection<InstanceGroup> instanceGroups) {
        return instanceGroups.stream().map(InstanceGroup::getId).collect(Collectors.toList());
    }

    private static List<String> instanceIds(Collection<Instance> instances) {
        return instances.stream().map(Instance::getId).collect(Collectors.toList());
    }
}