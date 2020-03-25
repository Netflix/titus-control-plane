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

package com.netflix.titus.master.agent.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceGroupUpdateEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.agent.ServerInfo;
import com.netflix.titus.master.agent.service.cache.AgentCache;
import com.netflix.titus.master.agent.service.cache.CacheUpdateEvent;
import com.netflix.titus.master.agent.service.cache.CacheUpdateType;
import com.netflix.titus.master.agent.service.server.ServerInfoResolver;
import com.netflix.titus.master.model.ResourceDimensions;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.Observable;
import rx.Single;
import rx.subjects.PublishSubject;

import static com.netflix.titus.testkit.model.agent.AgentGenerator.agentInstances;
import static com.netflix.titus.testkit.model.agent.AgentGenerator.agentServerGroups;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultAgentManagementServiceTest {

    private static final int INSTANCE_GROUP_COUNT = 2;

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final AgentManagementConfiguration configuration = mock(AgentManagementConfiguration.class);
    private final InstanceCloudConnector connector = mock(InstanceCloudConnector.class);
    private final AgentCache agentCache = mock(AgentCache.class);
    private final ServerInfoResolver serverInfoResolver = mock(ServerInfoResolver.class);

    private final List<AgentInstanceGroup> instanceGroups = agentServerGroups(Tier.Flex, 5).toList(INSTANCE_GROUP_COUNT);
    private final List<AgentInstance> instanceSet0 = new ArrayList<>();
    private final List<AgentInstance> instanceSet1 = new ArrayList<>();

    private final Predicate<AgentInstanceGroup> kubeSchedulerInstanceGroupPredicate = ig -> instanceGroups.get(INSTANCE_GROUP_COUNT - 1).getId().equals(ig.getId());

    private final DefaultAgentManagementService service = new DefaultAgentManagementService(configuration,
            kubeSchedulerInstanceGroupPredicate,
            connector,
            agentCache,
            serverInfoResolver,
            titusRuntime
    );

    private final PublishSubject<CacheUpdateEvent> agentCacheEventSubject = PublishSubject.create();
    private final ExtTestSubscriber<AgentEvent> eventSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        agentInstances(instanceGroups.get(0)).apply(instanceSet0::add, 5);
        agentInstances(instanceGroups.get(1)).apply(instanceSet1::add, 5);

        when(configuration.isInstanceGroupUpdateCapacityEnabled()).thenReturn(true);

        when(agentCache.getInstanceGroups()).thenReturn(instanceGroups);

        when(agentCache.getInstanceGroup(instanceGroups.get(0).getId())).thenReturn(instanceGroups.get(0));
        when(agentCache.findInstanceGroup(instanceGroups.get(0).getId())).thenReturn(Optional.of(instanceGroups.get(0)));

        when(agentCache.getInstanceGroup(instanceGroups.get(1).getId())).thenReturn(instanceGroups.get(1));
        when(agentCache.findInstanceGroup(instanceGroups.get(1).getId())).thenReturn(Optional.of(instanceGroups.get(1)));

        when(agentCache.getAgentInstances(instanceGroups.get(0).getId())).thenReturn(new HashSet<>(instanceSet0));
        when(agentCache.getAgentInstances(instanceGroups.get(1).getId())).thenReturn(new HashSet<>(instanceSet1));
        for (AgentInstance instance : CollectionsExt.merge(instanceSet0, instanceSet1)) {
            when(agentCache.getAgentInstance(instance.getId())).thenReturn(instance);
        }
        when(agentCache.events()).thenReturn(agentCacheEventSubject);

        when(connector.updateCapacity(any(), any(), any())).thenReturn(Completable.complete());

        service.events(false).subscribe(eventSubscriber);
    }

    @Test
    public void testAgentInstanceBelongsToFenzo() {
        assertThat(service.isOwnedByFenzo(instanceGroups.get(0))).isTrue();
        assertThat(service.isOwnedByFenzo(instanceGroups.get(1))).isFalse();
    }

    @Test
    public void testAgentInstanceToFenzo() {
        assertThat(service.isOwnedByFenzo(instanceSet0.get(0))).isTrue();
        assertThat(service.isOwnedByFenzo(instanceSet1.get(0))).isFalse();
    }

    @Test
    public void testFindAgentInstances() {
        List<AgentInstanceGroup> serverGroups = service.getInstanceGroups();
        assertThat(serverGroups).hasSize(2);

        String serverGroupId0 = serverGroups.get(0).getId();

        List<Pair<AgentInstanceGroup, List<AgentInstance>>> result = service.findAgentInstances(pair -> pair.getLeft().getId().equals(serverGroupId0));
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getLeft().getId()).isEqualTo(serverGroups.get(0).getId());
    }

    @Test
    public void testGetResourceLimitsWithNoAdjustment() {
        ResourceDimension result = service.getResourceLimits(instanceGroups.get(0).getInstanceType());
        assertThat(result).isEqualTo(instanceGroups.get(0).getResourceDimension());
    }

    @Test
    public void testGetResourceLimitsWithAdjustment() {
        String instanceType = instanceGroups.get(0).getInstanceType();
        ResourceDimension adjustedResources = ResourceDimensions.multiply(instanceGroups.get(0).getResourceDimension(), 0.5);

        when(serverInfoResolver.resolve(instanceType)).thenReturn(Optional.of(ServerInfo.from(adjustedResources)));

        ResourceDimension result = service.getResourceLimits(instanceType);
        assertThat(result).isEqualTo(adjustedResources);
    }

    @Test
    public void testUpdateTier() {
        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        AgentInstanceGroup instanceGroup = instanceGroups.get(0);
        String instanceGroupId = instanceGroup.getId();

        when(agentCache.updateInstanceGroupStore(eq(instanceGroupId), any()))
                .thenReturn(Single.just(instanceGroup.toBuilder().withTier(Tier.Critical).build()));
        service.updateInstanceGroupTier(instanceGroupId, Tier.Critical).toObservable().subscribe(testSubscriber);

        verify(agentCache, times(1)).updateInstanceGroupStore(eq(instanceGroupId), any());
    }

    @Test
    public void testUpdateLifecycle() {
        InstanceGroupLifecycleStatus updatedInstanceGroupLifecycleStatus = InstanceGroupLifecycleStatus.newBuilder().withState(InstanceGroupLifecycleState.Removable).build();

        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        AgentInstanceGroup instanceGroup = instanceGroups.get(0);
        String instanceGroupId = instanceGroup.getId();

        when(agentCache.updateInstanceGroupStore(eq(instanceGroupId), any()))
                .thenReturn(Single.just(instanceGroup.toBuilder().withLifecycleStatus(updatedInstanceGroupLifecycleStatus).build()));
        service.updateInstanceGroupLifecycle(instanceGroupId, updatedInstanceGroupLifecycleStatus).toObservable().subscribe(testSubscriber);

        verify(agentCache, times(1)).updateInstanceGroupStore(eq(instanceGroupId), any());
    }

    @Test
    public void testUpdateInstanceGroupAttributes() {
        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        AgentInstanceGroup instanceGroup = instanceGroups.get(0);
        String instanceGroupId = instanceGroup.getId();

        Map<String, String> attributes = Collections.singletonMap("a", "1");
        when(agentCache.updateInstanceGroupStore(eq(instanceGroupId), any()))
                .thenReturn(Single.just(instanceGroup.toBuilder().withAttributes(attributes).build()));
        service.updateInstanceGroupAttributes(instanceGroupId, attributes).toObservable().subscribe(testSubscriber);

        verify(agentCache, times(1)).updateInstanceGroupStore(eq(instanceGroupId), any());
    }

    @Test
    public void testUpdateAgentInstanceAttributes() {
        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        AgentInstance agentInstance = instanceSet0.get(0);
        String agentInstanceId = agentInstance.getId();
        assertThat(agentInstance.getAttributes()).isEmpty();
        Map<String, String> attributes = Collections.singletonMap("a", "1");
        when(agentCache.updateAgentInstanceStore(eq(agentInstanceId), any()))
                .thenReturn(Single.just(agentInstance.toBuilder().withAttributes(attributes).build()));
        service.updateAgentInstanceAttributes(agentInstance.getId(), attributes).toObservable().subscribe(testSubscriber);

        verify(agentCache, times(1)).updateAgentInstanceStore(eq(agentInstanceId), any());
    }

    @Test
    public void testUpdateCapacity() {
        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        AgentInstanceGroup instanceGroup = instanceGroups.get(0);
        String instanceGroupId = instanceGroup.getId();

        when(agentCache.updateInstanceGroupStoreAndSyncCloud(eq(instanceGroupId), any()))
                .thenReturn(Single.just(instanceGroup));

        service.updateCapacity(instanceGroups.get(0).getId(), Optional.of(100), Optional.of(1000)).toObservable().subscribe(testSubscriber);

        verify(connector, times(1)).updateCapacity(instanceGroups.get(0).getId(), Optional.of(100), Optional.of(1000));

        verify(agentCache, times(1)).updateInstanceGroupStoreAndSyncCloud(eq(instanceGroupId), any());
    }

    @Test
    public void testScaleUp() {
        ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();
        AgentInstanceGroup instanceGroup = instanceGroups.get(0);
        String instanceGroupId = instanceGroup.getId();

        when(agentCache.updateInstanceGroupStoreAndSyncCloud(eq(instanceGroupId), any()))
                .thenReturn(Single.just(instanceGroup));
        service.scaleUp(instanceGroup.getId(), 500).toObservable().subscribe(testSubscriber);

        verify(connector, times(1)).scaleUp(instanceGroup.getId(), 500);

        verify(agentCache, times(1)).updateInstanceGroupStoreAndSyncCloud(eq(instanceGroupId), any());
    }

    @Test
    public void testTerminateAgentsFromOneServerGroup() {
        String agentId1 = instanceSet0.get(0).getId();
        String agentId2 = instanceSet0.get(1).getId();
        List<String> agentIds = asList(agentId1, agentId2);

        when(agentCache.getAgentInstance(agentId1)).thenReturn(instanceSet0.get(0));
        when(agentCache.getAgentInstance(agentId2)).thenReturn(instanceSet0.get(1));
        when(connector.terminateInstances(instanceGroups.get(0).getId(), agentIds, false)).thenReturn(
                Observable.just(asList(Either.ofValue(true), Either.ofValue(true)))
        );
        when(agentCache.removeInstances(any(), any())).thenReturn(Completable.complete());

        ExtTestSubscriber<List<Either<Boolean, Throwable>>> testSubscriber = new ExtTestSubscriber<>();
        service.terminateAgents(instanceGroups.get(0).getId(), agentIds, false).subscribe(testSubscriber);

        List<Either<Boolean, Throwable>> result = testSubscriber.takeNext();
        assertThat(result).hasSize(2);
        verify(agentCache, times(1)).removeInstances(any(), any());
    }

    @Test
    public void testTerminateAgentsFromDifferentServerGroups() {
        String agentId1 = instanceSet0.get(0).getId();
        String agentId2 = instanceSet1.get(0).getId();
        List<String> agentIds = asList(agentId1, agentId2);

        when(agentCache.getAgentInstance(agentId1)).thenReturn(instanceSet0.get(0));
        when(agentCache.getAgentInstance(agentId2)).thenReturn(instanceSet1.get(0));

        ExtTestSubscriber<List<Either<Boolean, Throwable>>> testSubscriber = new ExtTestSubscriber<>();
        service.terminateAgents(instanceGroups.get(0).getId(), agentIds, false).subscribe(testSubscriber);

        assertThat(testSubscriber.isError()).isTrue();
    }

    @Test
    public void testEventOnServerGroupUpdate() {
        instanceGroups.set(0, instanceGroups.get(0).toBuilder().withMax(1000).build());
        agentCacheEventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroups.get(0).getId()));
        AgentEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(AgentInstanceGroupUpdateEvent.class);
    }

    @Test
    public void testEventOnServerGroupRemoved() {
        String id = instanceGroups.get(0).getId();
        instanceGroups.remove(0);

        agentCacheEventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, id));
        AgentEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(AgentInstanceGroupRemovedEvent.class);
    }

    @Test
    public void testEventOnServerUpdate() {
        instanceSet0.set(0, instanceSet0.get(0).toBuilder().withHostname("changed").build());
        when(agentCache.getAgentInstances(instanceGroups.get(0).getId())).thenReturn(new HashSet<>(instanceSet0));

        agentCacheEventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.Instance, instanceSet0.get(0).getId()));
        AgentEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(AgentInstanceUpdateEvent.class);
    }

    @Test
    public void testEventOnServerRemovedAndGroupRefresh() {
        instanceSet0.remove(0);
        when(agentCache.getAgentInstances(instanceGroups.get(0).getId())).thenReturn(new HashSet<>(instanceSet0));

        agentCacheEventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.InstanceGroup, instanceGroups.get(0).getId()));
        AgentEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(AgentInstanceRemovedEvent.class);
    }

    @Test
    public void testEventOnServerRemoved() {
        String id = instanceSet0.get(0).getId();
        instanceSet0.remove(0);
        when(agentCache.getAgentInstances(instanceGroups.get(0).getId())).thenReturn(new HashSet<>(instanceSet0));

        agentCacheEventSubject.onNext(new CacheUpdateEvent(CacheUpdateType.Instance, id));
        AgentEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(AgentInstanceRemovedEvent.class);
    }
}
