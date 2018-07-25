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

package com.netflix.titus.master.agent.service.monitor;

import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.master.mesos.model.Status;
import com.netflix.titus.master.StatusSamples;
import com.netflix.titus.master.job.worker.WorkerStateMonitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static com.netflix.titus.master.StatusSamples.SAMPLE_AGENT_1;
import static com.netflix.titus.master.StatusSamples.SAMPLE_AGENT_2;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class V2JobStatusMonitorTest {

    private static final AgentStatus JOB_STATUS_OK = AgentStatus.healthy(V2JobStatusMonitor.SOURCE_ID, SAMPLE_AGENT_1, V2JobStatusMonitor.HEALTHY_MESSAGE, 0);
    private static final AgentStatus JOB2_STATUS_OK = AgentStatus.healthy(V2JobStatusMonitor.SOURCE_ID, SAMPLE_AGENT_2, V2JobStatusMonitor.HEALTHY_MESSAGE, 0);

    private static final AgentStatus JOB_STATUS_BAD = AgentStatus.unhealthy(
            V2JobStatusMonitor.SOURCE_ID, StatusSamples.SAMPLE_AGENT_1, V2JobStatusMonitor.UNHEALTHY_MESSAGE, 0
    );
    private static final AgentStatus JOB_STATUS_TERMINATED = AgentStatus.terminated(V2JobStatusMonitor.SOURCE_ID, StatusSamples.SAMPLE_AGENT_1, V2JobStatusMonitor.TERMINATED_MESSAGE, 0);

    private final TestScheduler testScheduler = Schedulers.test();
    private final MonitorTestSubscriber monitorSubscriber = new MonitorTestSubscriber();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final WorkerStateMonitor workerStateMonitor = mock(WorkerStateMonitor.class);

    private final PublishSubject<AgentEvent> agentUpdateSubject = PublishSubject.create();
    private final PublishSubject<Status> statusObservable = PublishSubject.create();

    private Subscription monitorSubscription;

    @Before
    public void setUp() throws Exception {
        when(agentManagementService.getAgentInstance(SAMPLE_AGENT_1.getId())).thenReturn(SAMPLE_AGENT_1);
        when(agentManagementService.getAgentInstance(SAMPLE_AGENT_2.getId())).thenReturn(SAMPLE_AGENT_2);
        when(agentManagementService.events(true)).thenReturn(agentUpdateSubject);

        when(workerStateMonitor.getAllStatusObservable()).thenReturn(statusObservable);
        V2JobStatusMonitor monitor = new V2JobStatusMonitor(DefaultConfiguration.CONFIG, agentManagementService, workerStateMonitor, new DefaultRegistry(), testScheduler);
        monitor.enterActiveMode();

        monitorSubscription = monitor.monitor().subscribe(monitorSubscriber);
    }

    @After
    public void tearDown() throws Exception {
        if (monitorSubscription != null) {
            monitorSubscription.unsubscribe();
        }
    }

    @Test
    public void testSuccessfulStatusUpdatesKeepAgentInUse() throws Exception {
        statusObservable.onNext(StatusSamples.STATUS_STARTED);
        testScheduler.triggerActions();
        monitorSubscriber.verifyEmitted(JOB_STATUS_OK);

        // Application level failures should have no impact
        runTimes(DefaultConfiguration.CONFIG.getFailingAgentErrorCheckCount() + 1, () -> statusObservable.onNext(StatusSamples.STATUS_FAILED));
        testScheduler.triggerActions();
        monitorSubscriber.verifyNothingEmitted();
    }

    @Test
    public void testInfrequentFailuresDoNotIsolateAgentNode() throws Exception {
        runTimes(DefaultConfiguration.CONFIG.getFailingAgentErrorCheckCount() + 1, () -> {
            statusObservable.onNext(StatusSamples.STATUS_CRASHED);
            testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getFailingAgentErrorCheckWindow(), TimeUnit.MILLISECONDS);
        });

        assertThat(monitorSubscriber.getOnNextEvents().size(), is(equalTo(0)));
    }

    @Test
    public void testExcessiveFailuresIsolateAgentNode() throws Exception {
        runTimes(DefaultConfiguration.CONFIG.getFailingAgentErrorCheckCount() + 1, () -> statusObservable.onNext(StatusSamples.STATUS_CRASHED));
        testScheduler.triggerActions();

        monitorSubscriber.verifyEmitted(JOB_STATUS_BAD);

        statusObservable.onNext(StatusSamples.STATUS_CRASHED);
        monitorSubscriber.verifyEmitted(JOB_STATUS_BAD);
    }

    @Test
    public void testIsolatedNodeIsReenabledByFirstStartedJob() throws Exception {
        // Disable host first
        runTimes(DefaultConfiguration.CONFIG.getFailingAgentErrorCheckCount() + 1, () -> statusObservable.onNext(StatusSamples.STATUS_CRASHED));
        testScheduler.triggerActions();

        // Now send status STARTED
        statusObservable.onNext(StatusSamples.STATUS_STARTED);

        monitorSubscriber.verifyEmitted(JOB_STATUS_BAD, JOB_STATUS_OK);
    }

    @Test
    public void testDeadNodeSubscriptionIsTerminated() throws Exception {
        statusObservable.onNext(StatusSamples.STATUS_STARTED);
        statusObservable.onNext(StatusSamples.STATUS_2_STARTED);
        testScheduler.triggerActions();
        monitorSubscriber.verifyEmitted(JOB_STATUS_OK, JOB2_STATUS_OK);

        agentUpdateSubject.onNext(new AgentInstanceRemovedEvent(StatusSamples.STATUS_STARTED.getInstanceId()));
        monitorSubscriber.verifyEmitted(JOB_STATUS_TERMINATED);
    }

    private void runTimes(int count, Runnable runnable) {
        for (int i = 0; i < count; i++) {
            runnable.run();
        }
    }
}