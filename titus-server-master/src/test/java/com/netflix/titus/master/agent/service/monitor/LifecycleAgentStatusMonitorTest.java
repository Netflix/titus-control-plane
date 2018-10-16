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

import java.util.Optional;

import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.agent.model.event.AgentEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceRemovedEvent;
import com.netflix.titus.api.agent.model.event.AgentInstanceUpdateEvent;
import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.api.agent.service.AgentManagementException.ErrorCode;
import com.netflix.titus.api.agent.service.AgentManagementService;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.testkit.model.agent.AgentGenerator;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.Test;
import rx.subjects.PublishSubject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LifecycleAgentStatusMonitorTest {

    private static final AgentInstance STARTED_INSTANCE = AgentGenerator.agentInstances().getValue().toBuilder()
            .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Started).build())
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final AgentManagementService agentManagementService = mock(AgentManagementService.class);

    private final LifecycleAgentStatusMonitor monitor = new LifecycleAgentStatusMonitor(agentManagementService, titusRuntime);

    @Test
    public void testGetHealthAndStatusForExistingInstance() {
        when(agentManagementService.findAgentInstance("good")).thenReturn(Optional.of(STARTED_INSTANCE));

        assertThat(monitor.isHealthy("good")).isTrue();

        AgentStatus healthState = monitor.getStatus("good");
        assertThat(healthState).isNotNull();
        assertThat(healthState.getStatusCode()).isEqualTo(AgentStatusCode.Healthy);
    }

    @Test
    public void testGetHealthAndStatusForNotExistingInstance() {
        when(agentManagementService.findAgentInstance("notFound")).thenReturn(Optional.empty());

        assertThat(monitor.isHealthy("notFound")).isFalse();

        Optional<Throwable> error = ExceptionExt.doCatch(() -> monitor.getStatus("notFound"));
        assertThat(error).containsInstanceOf(AgentManagementException.class);
        assertThat(((AgentManagementException) error.get()).getErrorCode()).isEqualTo(ErrorCode.AgentNotFound);
    }

    @Test
    public void testEventStream() {
        PublishSubject<AgentEvent> eventSubject = PublishSubject.create();
        when(agentManagementService.events(false)).thenReturn(eventSubject);

        ExtTestSubscriber<AgentStatus> testSubscriber = new ExtTestSubscriber<>();
        monitor.monitor().subscribe(testSubscriber);

        // First started
        eventSubject.onNext(new AgentInstanceUpdateEvent(STARTED_INSTANCE));
        assertThat(testSubscriber.takeNext().getStatusCode()).isEqualTo(AgentStatusCode.Healthy);

        // Now terminated
        eventSubject.onNext(new AgentInstanceRemovedEvent(STARTED_INSTANCE.getId()));
        AgentStatus terminatedStatus = testSubscriber.takeNext();
        assertThat(terminatedStatus.getStatusCode()).isEqualTo(AgentStatusCode.Terminated);
        assertThat(terminatedStatus.getAgentInstance().getId()).isEqualTo(STARTED_INSTANCE.getId());
        assertThat(terminatedStatus.getAgentInstance().getLifecycleStatus().getState()).isEqualTo(InstanceLifecycleState.Stopped);
    }
}
