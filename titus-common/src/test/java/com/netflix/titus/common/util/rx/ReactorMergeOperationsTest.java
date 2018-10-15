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