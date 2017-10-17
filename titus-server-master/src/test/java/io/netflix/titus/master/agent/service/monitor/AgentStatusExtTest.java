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

import io.netflix.titus.api.agent.model.monitor.AgentStatus;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static io.netflix.titus.api.agent.model.monitor.AgentStatusExt.effectiveDisableTime;
import static io.netflix.titus.api.agent.model.monitor.AgentStatusExt.evaluateEffectiveStatus;
import static io.netflix.titus.api.agent.model.monitor.AgentStatusExt.isDifferent;
import static io.netflix.titus.api.agent.model.monitor.AgentStatusExt.isExpired;
import static io.netflix.titus.master.agent.service.monitor.AgentStatusSamples.DEFAULT_DISABLE_TIME;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 */
public class AgentStatusExtTest {

    private static final TestScheduler testScheduler = Schedulers.test();

    private final AgentStatusSamples samples = new AgentStatusSamples("testSource", testScheduler);

    @Test
    public void testEffectiveDisableTime() throws Exception {
        AgentStatus bad = samples.getBad();
        assertThat(effectiveDisableTime(bad, testScheduler), is(equalTo(DEFAULT_DISABLE_TIME)));

        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        assertThat(effectiveDisableTime(bad, testScheduler), is(equalTo(DEFAULT_DISABLE_TIME - 1)));
    }

    @Test
    public void testIsExpired() throws Exception {
        AgentStatus bad = samples.getBad();
        assertThat(isExpired(bad, testScheduler), is(false));

        testScheduler.advanceTimeBy(DEFAULT_DISABLE_TIME, TimeUnit.MILLISECONDS);
        assertThat(isExpired(bad, testScheduler), is(true));
    }

    @Test
    public void testIsDifferent() throws Exception {
        AgentStatus ok = samples.getOk();
        AgentStatus bad = samples.getBad();
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        AgentStatus badLater = samples.getBad();

        assertThat(isDifferent(ok, ok, testScheduler), is(false));
        assertThat(isDifferent(ok, bad, testScheduler), is(true));
        assertThat(isDifferent(bad, bad, testScheduler), is(false));
        assertThat(isDifferent(bad, badLater, testScheduler), is(true));
    }

    @Test
    public void testEvaluateEffectiveStatus() throws Exception {
        AgentStatus ok = samples.getOk();
        AgentStatus bad = samples.getBad();
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        AgentStatus badLater = samples.getBad();

        assertThat(evaluateEffectiveStatus(asList(ok, ok), testScheduler), is(equalTo(ok)));
        assertThat(evaluateEffectiveStatus(asList(ok, bad), testScheduler), is(equalTo(bad)));
        assertThat(evaluateEffectiveStatus(asList(bad, badLater), testScheduler), is(equalTo(badLater)));
    }
}