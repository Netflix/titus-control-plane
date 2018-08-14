package com.netflix.titus.master.supervisor.service;

import java.util.List;

import com.netflix.governator.LifecycleManager;
import com.netflix.titus.common.data.generator.MutableDataGenerator;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.model.event.MasterInstanceRemovedEvent;
import com.netflix.titus.master.supervisor.model.event.MasterInstanceUpdateEvent;
import com.netflix.titus.master.supervisor.model.event.SupervisorEvent;
import com.netflix.titus.testkit.model.supervisor.MasterInstanceGenerator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.subjects.PublishSubject;

import static com.netflix.titus.testkit.model.supervisor.MasterInstanceGenerator.masterInstances;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultSupervisorOperationsTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final MasterMonitor masterMonitor = mock(MasterMonitor.class);
    private final PublishSubject<List<MasterInstance>> masterMonitorSubject = PublishSubject.create();

    private DefaultSupervisorOperations supervisorOperations;

    @Before
    public void setUp() throws Exception {
        when(masterMonitor.observeMasters()).thenReturn(masterMonitorSubject);

        supervisorOperations = new DefaultSupervisorOperations(new LifecycleManager(), masterMonitor, titusRuntime);
    }

    @Test
    public void testEventStream() {
        ExtTestSubscriber<SupervisorEvent> eventSubscriber = new ExtTestSubscriber<>();
        supervisorOperations.events().subscribe(eventSubscriber);

        List<MasterInstance> initialInstances = new MutableDataGenerator<>(
                masterInstances(MasterState.Starting, "id1", "id2", "id3")
        ).getValues(3);

        // Initial set
        for (int i = 0; i < 3; i++) {
            masterMonitorSubject.onNext(initialInstances.subList(0, i + 1));
            expectMasterInstanceUpdateEvent(eventSubscriber, initialInstances.get(i));
        }

        // Change state of first instance
        List<MasterInstance> firstUpdated = asList(
                MasterInstanceGenerator.moveTo(initialInstances.get(0), MasterState.NonLeader),
                initialInstances.get(1),
                initialInstances.get(2)
        );
        masterMonitorSubject.onNext(firstUpdated);
        expectMasterInstanceUpdateEvent(eventSubscriber, firstUpdated.get(0));

        // Remove last instance
        masterMonitorSubject.onNext(firstUpdated.subList(0, 2));
        expectMasterInstanceRemovedEvent(eventSubscriber, firstUpdated.get(2));
    }

    private void expectMasterInstanceUpdateEvent(ExtTestSubscriber<SupervisorEvent> eventSubscriber, MasterInstance instance) {
        SupervisorEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(MasterInstanceUpdateEvent.class);

        MasterInstanceUpdateEvent updateEvent = (MasterInstanceUpdateEvent) event;
        assertThat(updateEvent.getMasterInstance()).isEqualTo(instance);
    }

    private void expectMasterInstanceRemovedEvent(ExtTestSubscriber<SupervisorEvent> eventSubscriber, MasterInstance removed) {
        SupervisorEvent event = eventSubscriber.takeNext();
        assertThat(event).isInstanceOf(MasterInstanceRemovedEvent.class);

        MasterInstanceRemovedEvent removedEvent = (MasterInstanceRemovedEvent) event;
        assertThat(removedEvent.getMasterInstance()).isEqualTo(removed);
    }
}