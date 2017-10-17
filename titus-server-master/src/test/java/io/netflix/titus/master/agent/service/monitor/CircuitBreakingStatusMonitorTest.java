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

import java.util.List;

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

public class CircuitBreakingStatusMonitorTest {

    private static final AgentInstance AGENT_1;
    private static final AgentInstance AGENT_2;

    static {
        List<AgentInstance> instances = AgentGenerator.agentInstances().toList(2);
        AGENT_1 = instances.get(0);
        AGENT_2 = instances.get(1);
    }

    private static final AgentStatus AGENT_1_STATUS_OK = AgentStatus.healthy("delegate", AGENT_1);
    private static final AgentStatus AGENT_1_STATUS_BAD = AgentStatus.unhealthy("delegate", AGENT_1, 60000, System.currentTimeMillis());
    private static final AgentStatus AGENT_2_STATUS_BAD = AgentStatus.unhealthy("delegate", AGENT_2, 60000, System.currentTimeMillis());

    private final TestScheduler testScheduler = Schedulers.test();

    private final AgentStatusMonitor delegate = mock(AgentStatusMonitor.class);
    private final PublishSubject<AgentStatus> delegateStatusSubject = PublishSubject.create();

    private int thresholdPerc = 20;
    private int agentCount = 5;

    private final AgentStatusMonitor statusMonitor = new CircuitBreakingStatusMonitor(
            delegate, () -> thresholdPerc, () -> agentCount, testScheduler
    );

    private final ExtTestSubscriber<AgentStatus> statusSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        when(delegate.monitor()).thenReturn(delegateStatusSubject);
        statusMonitor.monitor().subscribe(statusSubscriber);
    }

    @Test
    public void testNumberOfDisabledAgentsIsBelowThreshold() throws Exception {
        publishAndVerify(AGENT_1_STATUS_OK, AGENT_1_STATUS_OK);
        publishAndVerify(AGENT_1_STATUS_BAD, AGENT_1_STATUS_BAD);
        publishAndVerifyNoEmit(AGENT_2_STATUS_BAD);

        // Agent1 -> healthy, agent2 -> unhealthy
        publishAndVerify(AGENT_1_STATUS_OK, AGENT_1_STATUS_OK);
        publishAndVerify(AGENT_2_STATUS_BAD, AGENT_2_STATUS_BAD);
        publishAndVerifyNoEmit(AGENT_1_STATUS_BAD);

        // Now increase the threshold
        thresholdPerc = 40;
        publishAndVerify(AGENT_1_STATUS_BAD, AGENT_1_STATUS_BAD);
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