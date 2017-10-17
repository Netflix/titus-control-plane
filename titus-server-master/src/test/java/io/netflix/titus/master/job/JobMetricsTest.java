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

package io.netflix.titus.master.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.spectator.api.Measurement;
import com.netflix.spectator.api.Meter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Tag;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.api.store.v2.V2WorkerMetadata;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobMetricsTest {

    private final Registry registry = new DefaultRegistry();

    @Test
    public void testContainerLifecycleMetrics() throws Exception {
        JobMetrics jobMetrics = new JobMetrics("myJob", true, "myApp", "myCapacityGroup", registry);

        V2WorkerMetadata task = mock(V2WorkerMetadata.class);
        when(task.getJobId()).thenReturn("myJob");
        when(task.getWorkerInstanceId()).thenReturn("myInstanceId");
        when(task.getWorkerIndex()).thenReturn(1);
        when(task.getWorkerNumber()).thenReturn(10);

        // Accepted
        when(task.getState()).thenReturn(V2JobState.Accepted);
        jobMetrics.updateTaskMetrics(task);
        verifyCurrentState(task, V2JobState.Accepted);

        // Started
        when(task.getState()).thenReturn(V2JobState.Started);
        jobMetrics.updateTaskMetrics(task);
        verifyCurrentState(task, V2JobState.Started);

        // Finished
        when(task.getState()).thenReturn(V2JobState.Completed);
        jobMetrics.updateTaskMetrics(task);
        verifyCurrentState(task, V2JobState.Started, 0); // Terminated state has no currentState metrics
    }

    private void verifyCurrentState(V2WorkerMetadata task, V2JobState expectedState) {
        Set<V2JobState> zeroed = EnumSet.allOf(V2JobState.class);
        zeroed.remove(expectedState);

        for (V2JobState zeroedState : zeroed) {
            verifyCurrentState(task, zeroedState, 0);
        }
        verifyCurrentState(task, expectedState, 1);
    }

    private void verifyCurrentState(V2WorkerMetadata task, V2JobState expectedState, long expectedValue) {
        List<Measurement> measurements = valueOf(task, expectedState);
        if (expectedValue == 0 && measurements.isEmpty()) {
            return;
        }
        assertThat(measurements).isNotEmpty();
        Measurement last = measurements.get(0);
        assertThat(last.value()).isEqualTo((double) expectedValue);
    }

    private List<Measurement> valueOf(V2WorkerMetadata task, V2JobState state) {
        Optional<Meter> meter = registry.stream()
                .filter(m -> m.id().name().contains("currentState"))
                .filter(m -> {
                    Iterator<Tag> tagIt = m.id().tags().iterator();
                    boolean expectedId = false;
                    boolean expectedState = false;
                    while (tagIt.hasNext()) {
                        Tag tag = tagIt.next();
                        if (tag.key().equals("t.taskInstanceId") && tag.value().equals(task.getWorkerInstanceId())) {
                            expectedId = true;
                        }
                        if (tag.key().equals("state") && tag.value().equals(state.name())) {
                            expectedState = true;
                        }
                    }
                    return expectedId && expectedState;
                })
                .findFirst();
        return meter.map(m -> {
            Iterable<Measurement> it = m.measure();
            List<Measurement> measurements = new ArrayList<>();
            it.forEach(measurements::add);
            return measurements;
        }).orElse(Collections.emptyList());
    }
}