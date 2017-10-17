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

package io.netflix.titus.master.agent.service.monitor;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.spectator.api.DefaultRegistry;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentManagementService;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregatingAgentStatusMonitorTest {

    private final TestScheduler testScheduler = Schedulers.test();
    private final MonitorTestSubscriber monitorSubscriber = new MonitorTestSubscriber();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final MockedDelegate delegate1 = new MockedDelegate("delegate1");
    private final MockedDelegate delegate2 = new MockedDelegate("delegate2");

    private final AtomicReference<String> disconnectedHostRef = new AtomicReference<>();

    @Before
    public void setUp() throws Exception {
        when(agentManagementService.events(false)).thenReturn(Observable.never());
        
        Set<AgentStatusMonitor> delegates = asSet(delegate1.delegate, delegate2.delegate);
        AggregatingAgentStatusMonitor monitor = new AggregatingAgentStatusMonitor(delegates, DefaultConfiguration.CONFIG, agentManagementService, new DefaultRegistry(), testScheduler) {
            @Override
            protected void agentDisconnected(String hostname) {
                super.agentDisconnected(hostname);
                disconnectedHostRef.set(hostname);
            }
        };
        monitor.enterActiveMode();
        monitor.monitor().subscribe(monitorSubscriber);
    }

    @Test
    public void testOkStatusValuesMergeIntoOkStatus() throws Exception {
        delegate1.nextOk();
        monitorSubscriber.verifyEmittedOk();

        delegate2.nextOk();
        monitorSubscriber.verifyNothingEmitted();
    }

    @Test
    public void testOkStatusAndBadStatusResultInBadStatus() throws Exception {
        delegate1.nextBad();
        monitorSubscriber.verifyEmittedBad();

        delegate2.nextOk();
        monitorSubscriber.verifyNothingEmitted();
    }

    @Test
    public void testBadStatusesResultInBadStatusWithBiggestEffectiveDisableTime() throws Exception {
        delegate1.nextBad();
        monitorSubscriber.verifyEmittedBad();

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        delegate2.nextBad();
        monitorSubscriber.verifyEmittedBad();
    }

    @Test
    public void testBadStatusWithShortDelayDoesNotInvalidatePendingBadStatusWithLongerDelay() {
        delegate1.nextBad();
        monitorSubscriber.verifyEmittedBad();

        delegate2.nextBad(AgentStatusSamples.DEFAULT_DISABLE_TIME / 2);
        monitorSubscriber.verifyNothingEmitted();
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
        delegate2.nextOk();

        // Advance time, to cross the timeout
        testScheduler.advanceTimeBy(DefaultConfiguration.CONFIG.getDeadAgentTimeout(), TimeUnit.MILLISECONDS);

        assertThat(disconnectedHostRef.get(), is(notNullValue()));
        assertThat(monitorSubscriber.isUnsubscribed(), is(false));
    }

    private class MockedDelegate {
        AgentStatusMonitor delegate = mock(AgentStatusMonitor.class);

        volatile Subscriber<? super AgentStatus> currentSubscriber;

        Observable<AgentStatus> monitorObservable = Observable.create(subscriber -> {
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
        }

        void nextOk() {
            if (currentSubscriber != null) {
                currentSubscriber.onNext(samples.getOk());
            }
        }

        void nextBad() {
            if (currentSubscriber != null) {
                currentSubscriber.onNext(samples.getBad());
                currentSubscriber = null;
            }
        }

        void nextBad(long disableTime) {
            if (currentSubscriber != null) {
                currentSubscriber.onNext(samples.getBad(disableTime));
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