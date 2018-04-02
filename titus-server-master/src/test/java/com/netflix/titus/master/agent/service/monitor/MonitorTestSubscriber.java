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

import java.util.List;

import com.netflix.titus.api.agent.model.monitor.AgentStatus;
import com.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import rx.observers.TestSubscriber;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

class MonitorTestSubscriber extends TestSubscriber<AgentStatus> {

    private int offset;

    public void verifyEmitted(AgentStatus... expected) {
        List<AgentStatus> allEmitted = getOnNextEvents();
        assertThat(allEmitted.size() - offset, is(equalTo(expected.length)));

        List<AgentStatus> emitted = allEmitted.subList(offset, allEmitted.size());
        for (int i = 0; i < expected.length; i++) {
            assertThat(emitted.get(i), is(equalTo(expected[i])));
        }

        offset += expected.length;
    }

    public void verifyEmittedOk() {
        verifyEmitted(AgentStatusCode.Healthy);
    }

    public void verifyEmittedTerminated() {
        verifyEmitted(AgentStatusCode.Terminated);
    }

    public void verifyEmittedBad() {
        verifyEmitted(AgentStatusCode.Unhealthy);
    }

    public void verifyNothingEmitted() {
        List<AgentStatus> allEvents = getOnNextEvents();
        assertThat("Unexpected events: " + allEvents.subList(offset, allEvents.size()), allEvents.size(), is(equalTo(offset)));
    }

    private void verifyEmitted(AgentStatusCode statusCode) {
        List<AgentStatus> allEmitted = getOnNextEvents();
        assertThat(allEmitted.size() - offset >= 1, is(true));

        assertThat(allEmitted.get(offset).getStatusCode(), is(equalTo(statusCode)));

        offset += 1;
    }
}
