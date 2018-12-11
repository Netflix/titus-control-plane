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

package com.netflix.titus.master.supervisor.service.leader;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.master.supervisor.SupervisorConfiguration;
import com.netflix.titus.master.supervisor.model.MasterInstance;
import com.netflix.titus.master.supervisor.model.MasterState;
import com.netflix.titus.master.supervisor.service.LeaderElector;
import com.netflix.titus.master.supervisor.service.LocalMasterInstanceResolver;
import com.netflix.titus.master.supervisor.service.MasterMonitor;
import com.netflix.titus.testkit.model.supervisor.MasterInstanceGenerator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import rx.Completable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static com.netflix.titus.testkit.model.supervisor.MasterInstanceGenerator.moveTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LeaderElectionOrchestratorTest {

    private final TestScheduler testScheduler = Schedulers.test();

    private final TitusRuntime titusRuntime = TitusRuntimes.test(testScheduler);

    private final SupervisorConfiguration configuration = mock(SupervisorConfiguration.class);

    private final LocalMasterInstanceResolver localMasterInstanceResolver = mock(LocalMasterInstanceResolver.class);
    private final PublishSubject<MasterInstance> localMasterUpdates = PublishSubject.create();

    private final MasterMonitor masterMonitor = mock(MasterMonitor.class);
    private final InOrder masterMonitorInOrder = inOrder(masterMonitor);

    private final LeaderElector leaderElector = mock(LeaderElector.class);
    private final PublishSubject<MasterState> leaderUpdates = PublishSubject.create();

    private LeaderElectionOrchestrator orchestrator;

    private MasterInstance localMasterInstance = MasterInstanceGenerator.getLocalMasterInstance(MasterState.Starting);

    @Before
    public void setUp() throws Exception {
        when(localMasterInstanceResolver.observeLocalMasterInstanceUpdates()).thenReturn(localMasterUpdates);
        when(leaderElector.awaitElection()).thenReturn(leaderUpdates);
        resetMasterMonitor();

        orchestrator = new LeaderElectionOrchestrator(
                configuration,
                localMasterInstanceResolver,
                masterMonitor,
                leaderElector,
                localMasterInstance,
                titusRuntime,
                testScheduler
        );

        verify(masterMonitor, times(1)).updateOwnMasterInstance(localMasterInstance);
        resetMasterMonitor();
    }

    @After
    public void tearDown() {
        if (orchestrator != null) {
            orchestrator.shutdown();
        }
    }

    @Test
    public void testForceLeaderElection() {
        when(configuration.isForceLeaderElectionEnabled()).thenReturn(true);

        orchestrator = new LeaderElectionOrchestrator(
                configuration,
                localMasterInstanceResolver,
                masterMonitor,
                leaderElector,
                localMasterInstance,
                titusRuntime,
                testScheduler
        );

        verify(leaderElector, times(1)).join();
    }

    @Test
    public void testBootstrapSequence() {
        // Change state to 'NonLeader'
        localMasterUpdates.onNext((localMasterInstance = moveTo(localMasterInstance, MasterState.NonLeader)));
        masterMonitorInOrder.verify(masterMonitor, times(1)).updateOwnMasterInstance(localMasterInstance);
        verify(leaderElector, times(1)).join();

        // Start leader activation process
        leaderUpdates.onNext(MasterState.LeaderActivating);
        verifyUpdatedOwnInstanceTo(MasterState.LeaderActivating);

        // Move to the activated state
        leaderUpdates.onNext(MasterState.LeaderActivated);
        verifyUpdatedOwnInstanceTo(MasterState.LeaderActivated);
    }

    @Test
    public void testDeactivation() {
        // Change state to 'NonLeader'
        localMasterUpdates.onNext((localMasterInstance = moveTo(localMasterInstance, MasterState.NonLeader)));
        masterMonitorInOrder.verify(masterMonitor, times(1)).updateOwnMasterInstance(localMasterInstance);
        verify(leaderElector, times(1)).join();

        // Change back to 'Inactive'
        localMasterUpdates.onNext((localMasterInstance = moveTo(localMasterInstance, MasterState.Inactive)));
        masterMonitorInOrder.verify(masterMonitor, times(1)).updateOwnMasterInstance(localMasterInstance);
        verify(leaderElector, times(1)).leaveIfNotLeader();
    }

    private void resetMasterMonitor() {
        Mockito.reset(masterMonitor);
        when(masterMonitor.getCurrentMasterInstance()).thenAnswer(invocation -> localMasterInstance);
        when(masterMonitor.updateOwnMasterInstance(any())).thenReturn(Completable.complete());
    }

    private void verifyUpdatedOwnInstanceTo(MasterState expectedState) {
        ArgumentCaptor<MasterInstance> captor = ArgumentCaptor.forClass(MasterInstance.class);
        masterMonitorInOrder.verify(masterMonitor, times(1)).updateOwnMasterInstance(captor.capture());
        assertThat(captor.getValue().getStatus().getState()).isEqualTo(expectedState);
    }
}