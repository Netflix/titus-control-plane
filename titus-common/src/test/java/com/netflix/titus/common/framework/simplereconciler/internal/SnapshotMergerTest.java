/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import reactor.core.publisher.Flux;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotMergerTest {

    @Test
    public void testMergeSnapshots() {
        List<List<String>> stream1 = Arrays.asList(
                Arrays.asList("stream1_snapshot_value1", "stream1_snapshot_value2"),
                Arrays.asList("stream1_update_value1", "stream1_update_value2")
        );
        List<List<String>> stream2 = Arrays.asList(
                Arrays.asList("stream2_snapshot_value1", "stream2_snapshot_value2"),
                Arrays.asList("stream2_update_value1", "stream2_update_value2")
        );
        Flux<List<String>> events = SnapshotMerger.mergeWithSingleSnapshot(Arrays.asList(
                Flux.fromIterable(stream1),
                Flux.fromIterable(stream2)
        ));
        Iterator<List<String>> eventIt = events.toIterable().iterator();

        assertThat(eventIt.hasNext()).isTrue();
        List<String> snapshot = eventIt.next();
        assertThat(snapshot).containsExactly(
                "stream1_snapshot_value1", "stream1_snapshot_value2", "stream2_snapshot_value1", "stream2_snapshot_value2");

        assertThat(eventIt.hasNext()).isTrue();
        List<String> updates1 = eventIt.next();
        assertThat(updates1).containsExactly("stream1_update_value1", "stream1_update_value2");

        assertThat(eventIt.hasNext()).isTrue();
        List<String> updates2 = eventIt.next();
        assertThat(updates2).containsExactly("stream2_update_value1", "stream2_update_value2");
    }
}