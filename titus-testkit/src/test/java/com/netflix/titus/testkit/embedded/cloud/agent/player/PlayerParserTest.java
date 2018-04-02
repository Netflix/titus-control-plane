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

package com.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.simulator.TitusCloudSimulator.SimulatedTaskStatus.SimulatedTaskState;
import com.netflix.titus.common.util.NumberSequence;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PlayerParserTest {

    @Test
    public void testSingleRule() {
        Either<List<Pair<ContainerSelector, ContainerRules>>, String> parsed = PlayerParser.parseInternal(ImmutableMap.of(
                "TASK_LIFECYCLE_1", "selector: slots=5.. slotStep=2 resubmits=1..10 resubmitStep=3; launched: delay=2s; startInitiated: delay=3s; started: delay=60s; killInitiated: delay=5s"
        ));
        assertThat(parsed.hasValue()).isTrue();

        ContainerSelector selector = parsed.getValue().get(0).getLeft();
        expectSequenceStartingWith(selector.getSlots(), 5L, 7L);
        expectSequenceStartingWith(selector.getResubmits(), 1L, 4L, 7L);

        ContainerRules containerRules = parsed.getValue().get(0).getRight();
        assertThat(containerRules.getTaskStateRules()).hasSize(4);

        expectNonFailingStateRule(containerRules, SimulatedTaskState.Launched, 2_000);
        expectNonFailingStateRule(containerRules, SimulatedTaskState.StartInitiated, 3_000);
        expectNonFailingStateRule(containerRules, SimulatedTaskState.Started, 60_000);
        expectNonFailingStateRule(containerRules, SimulatedTaskState.Killed, 5_000);
    }

    private void expectSequenceStartingWith(NumberSequence sequence, long... expectedValues) {
        Iterator<Long> it = sequence.getIterator();
        for (long expected : expectedValues) {
            assertThat(it.next()).isEqualTo(expected);
        }
    }

    private void expectNonFailingStateRule(ContainerRules containerRules, SimulatedTaskState state, long expectedDelayMs) {
        ContainerStateRule rule = containerRules.getTaskStateRules().get(state);
        assertThat(rule).isNotNull();
        assertThat(rule.getDelayInStateMs()).isEqualTo(expectedDelayMs);
        assertThat(rule.getReasonCode()).isNotPresent();
    }
}
