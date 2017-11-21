package io.netflix.titus.master.agent.store;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.AgentInstanceGroup;
import io.netflix.titus.api.agent.store.AgentStore;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
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

    private final AgentStore agentStore = mock(AgentStore.class);

    private final AgentStoreReaper reaper = new AgentStoreReaper(agentStore, new DefaultRegistry(), testScheduler);

    @Before
    public void setUp() throws Exception {
        when(agentStore.retrieveAgentInstanceGroups()).thenReturn(Observable.empty());
        when(agentStore.retrieveAgentInstances()).thenReturn(Observable.empty());
        when(agentStore.removeAgentInstanceGroups(any())).thenReturn(Completable.complete());
        when(agentStore.removeAgentInstances(any())).thenReturn(Completable.complete());
        reaper.enterActiveMode();
    }

    @Test
    public void testTaggedInstanceGroupsAreRemovedAfterDeadline() throws Exception {
        AgentInstanceGroup instanceGroup = AgentStoreReaper.tagToRemove(AgentGenerator.agentServerGroups().getValue(), testScheduler);
        AgentInstance instance = AgentGenerator.agentInstances(instanceGroup).getValue();

        when(agentStore.retrieveAgentInstanceGroups()).thenReturn(Observable.just(instanceGroup));
        when(agentStore.retrieveAgentInstances()).thenReturn(Observable.just(instance));

        testScheduler.advanceTimeBy(AgentStoreReaper.EXPIRED_DATA_RETENTION_PERIOD_MS - 1, TimeUnit.MILLISECONDS);
        verify(agentStore, times(0)).removeAgentInstanceGroups(Collections.singletonList(instanceGroup.getId()));
        verify(agentStore, times(0)).removeAgentInstances(Collections.singletonList(instance.getId()));

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        verify(agentStore, times(1)).removeAgentInstanceGroups(Collections.singletonList(instanceGroup.getId()));
        verify(agentStore, times(1)).removeAgentInstances(Collections.singletonList(instance.getId()));
    }
}