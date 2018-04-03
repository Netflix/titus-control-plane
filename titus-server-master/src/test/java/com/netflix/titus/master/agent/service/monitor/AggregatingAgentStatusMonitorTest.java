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

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.api.agent.service.AgentStatusMonitor;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingAgentStatusMonitorTest {

    private final TestScheduler testScheduler = Schedulers.test();
    private final MonitorTestSubscriber monitorSubscriber = new MonitorTestSubscriber();

    private final MockedDelegate delegate1 = new MockedDelegate("delegate1");
    private final MockedDelegate delegate2 = new MockedDelegate("delegate2");

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    @Before
    public void setUp() throws Exception {
        Set<AgentStatusMonitor> delegates = asSet(delegate1.delegate, delegate2.delegate);
        AggregatingAgentStatusMonitor monitor = new AggregatingAgentStatusMonitor(delegates, agentManagementService, new DefaultRegistry(), testScheduler);
        monitor.enterActiveMode();
        monitor.monitor().subscribe(monitorSubscriber);
    }

    @Test
    public void testOkStatusValuesMergeIntoOkStatus() throws Exception {
        delegate1.nextOk();
        monitorSubscriber.verifyEmittedOk();

        delegate2.nextOk();
        monitorSubscriber.verifyEmittedOk();
    }

    @Test
    public void testOkStatusAndBadStatusResultInBadStatus() throws Exception {
        delegate1.nextBad();
        monitorSubscriber.verifyEmittedBad();

        delegate2.nextOk();
        monitorSubscriber.verifyEmittedBad();
    }

    @Test
    public void testExceptionInDelegateDoesNotBreakTheAggregate() throws Exception {
        delegate1.error();
        monitorSubscriber.assertNoTerminalEvent();

        // There should be no event before retry interval passes
        testScheduler.advanceTimeBy(AggregatingAgentStatusMonitor.MAX_RETRY_INTERVAL_MS - 1, TimeUnit.MILLISECONDS);
        delegate1.nextOk();
        monitorSubscriber.assertNoTerminalEvent();

        // Now cross the retry period
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        delegate1.nextOk();
        monitorSubscriber.verifyEmittedOk();
    }

    @Test
    public void testDeadNodeSubscriptionIsTerminated() throws Exception {
        delegate1.nextOk();
        monitorSubscriber.verifyEmittedOk();
        delegate2.nextOk();
        monitorSubscriber.verifyEmittedOk();

        delegate1.nextTerminate();
        monitorSubscriber.verifyEmittedTerminated();
        assertThat(monitorSubscriber.isUnsubscribed(), is(false));
    }

    private class MockedDelegate {
        AgentStatusMonitor delegate = mock(AgentStatusMonitor.class);

        volatile Subscriber<? super AgentStatus> currentSubscriber;

        Observable<AgentStatus> monitorObservable = Observable.unsafeCreate(subscriber -> {
            if (currentSubscriber != null) {
                currentSubscriber.onError(new IllegalStateException("Unexpected subsequent subscription"));
            } else {
                currentSubscriber = subscriber;
            }
        });

        AgentStatusSamples samples;

        MockedDelegate(String delegateId) {
            samples = new AgentStatusSamples(delegateId, testScheduler);
            when(delegate.monitor()).thenReturn(monitorObservable);
            when(delegate.getStatus(anyString())).thenReturn(samples.getOk());
        }

        void nextOk() {
            if (currentSubscriber != null) {
                AgentStatus ok = samples.getOk();
                when(delegate.getStatus(ok.getAgentInstance().getId())).thenReturn(ok);
                currentSubscriber.onNext(ok);
            }
        }

        void nextBad() {
            if (currentSubscriber != null) {
                AgentStatus bad = samples.getBad();
                when(delegate.getStatus(bad.getAgentInstance().getId())).thenReturn(bad);
                currentSubscriber.onNext(bad);
                currentSubscriber = null;
            }
        }

        void nextTerminate() {
            if (currentSubscriber != null) {
                AgentStatus terminated = samples.getTerminated();
                when(delegate.getStatus(terminated.getAgentInstance().getId())).thenReturn(terminated);
                currentSubscriber.onNext(terminated);
                currentSubscriber = null;
            }
        }

        void error() {
            if (currentSubscriber != null) {
                currentSubscriber.onError(new RuntimeException("simulated delegate error"));
                currentSubscriber = null;
            }
        }
    }
}