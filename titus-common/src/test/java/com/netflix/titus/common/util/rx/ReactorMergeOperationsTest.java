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

package com.netflix.titus.common.util.rx;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorMergeOperationsTest {

    @Test
    public void testMonoMerge() {
        Set<String> resultCollector = new HashSet<>();

        Map<String, Mono<Void>> monoMap = ImmutableMap.<String, Mono<Void>>builder()
                .put("mono1", Mono.defer(() -> {
                    resultCollector.add("mono1");
                    return Mono.empty();
                }))
                .put("mono2", Mono.defer(() -> {
                    resultCollector.add("mono2");
                    return Mono.empty();
                }))
                .build();

        Map<String, Optional<Throwable>> executionResult = ReactorExt.merge(monoMap, 2, Schedulers.parallel()).block();
        assertThat(executionResult).hasSize(2);
        assertThat(executionResult.get("mono1")).isEmpty();
        assertThat(executionResult.get("mono2")).isEmpty();
        assertThat(resultCollector).contains("mono1", "mono2");
    }

    @Test
    public void testMonoMergeWithError() {
        Map<String, Mono<Void>> monoMap = ImmutableMap.<String, Mono<Void>>builder()
                .put("mono1", Mono.defer(Mono::empty))
                .put("mono2", Mono.defer(() -> Mono.error(new RuntimeException("Simulated error"))))
                .put("mono3", Mono.defer(Mono::empty))
                .build();

        Map<String, Optional<Throwable>> executionResult = ReactorExt.merge(monoMap, 2, Schedulers.parallel()).block();
        assertThat(executionResult).hasSize(3);
        assertThat(executionResult.get("mono1")).isEmpty();
        assertThat(executionResult.get("mono2")).containsInstanceOf(RuntimeException.class);
        assertThat(executionResult.get("mono3")).isEmpty();
    }
}