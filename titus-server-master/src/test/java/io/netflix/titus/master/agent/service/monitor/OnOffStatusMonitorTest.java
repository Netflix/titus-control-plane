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

import java.util.concurrent.TimeUnit;

import io.netflix.titus.api.agent.model.AgentInstance;
import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import io.netflix.titus.api.agent.service.AgentStatusMonitor;
import io.netflix.titus.testkit.model.agent.AgentGenerator;
import io.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnOffStatusMonitorTest {

    private static final AgentInstance AGENT_1 = AgentGenerator.agentInstances().getValue();

    private static final AgentStatus AGENT_1_STATUS_OK = AgentStatus.healthy("delegate", AGENT_1);
    private static final AgentStatus AGENT_1_STATUS_BAD = AgentStatus.unhealthy("delegate", AGENT_1, 60000, System.currentTimeMillis());

    private final TestScheduler testScheduler = Schedulers.test();

    private final AgentStatusMonitor delegate = mock(AgentStatusMonitor.class);
    private final PublishSubject<AgentStatus> delegateStatusSubject = PublishSubject.create();

    private boolean on = true;

    private final AgentStatusMonitor onOffStatusMonitor = new OnOffStatusMonitor(delegate, () -> on, testScheduler);

    private final ExtTestSubscriber<AgentStatus> statusSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        when(delegate.monitor()).thenReturn(delegateStatusSubject);
        onOffStatusMonitor.monitor().subscribe(statusSubscriber);
    }

    @Test
    public void testOK() throws Exception {
        publishAndVerify(AGENT_1_STATUS_OK, AGENT_1_STATUS_OK);
        publishAndVerify(AGENT_1_STATUS_BAD, AGENT_1_STATUS_BAD);
    }

    @Test
    public void testOnOffTransitionWithOkFinalState() throws Exception {
        publishAndVerify(AGENT_1_STATUS_BAD, AGENT_1_STATUS_BAD);
        on = false;
        expectedStateEmit(AGENT_1_STATUS_OK);
        publishAndVerifyNoEmit(AGENT_1_STATUS_OK);

        on = true;
        expectedNoStateEmit();
    }

    @Test
    public void testOnOffTransitionWithBadFinalState() throws Exception {
        publishAndVerify(AGENT_1_STATUS_BAD, AGENT_1_STATUS_BAD);

        on = false;
        expectedStateEmit(AGENT_1_STATUS_OK);
        publishAndVerifyNoEmit(AGENT_1_STATUS_BAD);

        on = true;
        expectedStateEmit(AGENT_1_STATUS_BAD);
        publishAndVerify(AGENT_1_STATUS_OK, AGENT_1_STATUS_OK);
    }

    private void expectedStateEmit(AgentStatus expected) {
        testScheduler.advanceTimeBy(OnOffStatusMonitor.CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(statusSubscriber.takeNext()).isEqualTo(expected);
    }

    private void expectedNoStateEmit() {
        testScheduler.advanceTimeBy(OnOffStatusMonitor.CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
        assertThat(statusSubscriber.takeNext()).isNull();
    }

    private void publishAndVerify(AgentStatus toPublish, AgentStatus expected) {
        delegateStatusSubject.onNext(toPublish);
        testScheduler.triggerActions();
        assertThat(statusSubscriber.takeNext()).isEqualTo(expected);
    }

    private void publishAndVerifyNoEmit(AgentStatus toPublish) {
        delegateStatusSubject.onNext(toPublish);
        testScheduler.triggerActions();
        assertThat(statusSubscriber.takeNext()).isNull();
    }
}