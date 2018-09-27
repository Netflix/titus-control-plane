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

package com.netflix.titus.master.agent.store;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.store.AgentStore;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import org.junit.Before;
import org.junit.Test;
import rx.Completable;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AgentStoreReaperTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final AgentStoreReaperConfiguration configuration = mock(AgentStoreReaperConfiguration.class);

    private final AgentStore agentStore = mock(AgentStore.class);

    private final AgentStoreReaper reaper = new AgentStoreReaper(configuration, agentStore, new DefaultRegistry(), testScheduler);

    @Before
    public void setUp() throws Exception {
        when(configuration.getExpiredDataRetentionPeriodMs()).thenReturn(AgentStoreReaper.MIN_EXPIRED_DATA_RETENTION_PERIOD_MS);
        when(agentStore.retrieveAgentInstanceGroups()).thenReturn(Observable.empty());
        when(agentStore.retrieveAgentInstances()).thenReturn(Observable.empty());
        when(agentStore.removeAgentInstanceGroups(any())).thenReturn(Completable.complete());
        when(agentStore.removeAgentInstances(any())).thenReturn(Completable.complete());
        reaper.enterActiveMode();
    }

    @Test
    public void testTaggedInstanceGroupsAreRemovedAfterDeadline() {
        AgentInstanceGroup instanceGroup = AgentStoreReaper.tagToRemove(AgentGenerator.agentServerGroups().getValue(), testScheduler);
        AgentInstance instance = AgentGenerator.agentInstances(instanceGroup).getValue();

        when(agentStore.retrieveAgentInstanceGroups()).thenReturn(Observable.just(instanceGroup));
        when(agentStore.retrieveAgentInstances()).thenReturn(Observable.just(instance));

        testScheduler.advanceTimeBy(AgentStoreReaper.MIN_EXPIRED_DATA_RETENTION_PERIOD_MS - 1, TimeUnit.MILLISECONDS);
        verify(agentStore, times(0)).removeAgentInstanceGroups(Collections.singletonList(instanceGroup.getId()));
        verify(agentStore, times(0)).removeAgentInstances(Collections.singletonList(instance.getId()));

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        verify(agentStore, times(1)).removeAgentInstanceGroups(Collections.singletonList(instanceGroup.getId()));
        verify(agentStore, times(1)).removeAgentInstances(Collections.singletonList(instance.getId()));
    }
}