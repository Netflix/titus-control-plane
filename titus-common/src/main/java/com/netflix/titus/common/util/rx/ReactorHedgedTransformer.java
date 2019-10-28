/*
 * Copyright 2019 Netflix, Inc.
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.netflix.spectator.api.BasicTag;
import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.common.util.tuple.Pair;
import io.reactivex.exceptions.CompositeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

class ReactorHedgedTransformer<T> implements Function<Mono<T>, Mono<T>> {

    private static final Logger logger = LoggerFactory.getLogger(ReactorHedgedTransformer.class);

    private static final String METRICS_ROOT = "titus.reactor.hedged";

    private final List<Pair<Duration, Long>> thresholdSteps;

    private final Map<String, String> context;
    private final Registry registry;
    private final Scheduler scheduler;

    private final Id metricsId;

    private ReactorHedgedTransformer(List<Pair<Duration, Long>> thresholdSteps, Map<String, String> context, Registry registry, Scheduler scheduler) {
        this.thresholdSteps = thresholdSteps;
        this.context = context;
        this.registry = registry;
        this.scheduler = scheduler;

        this.metricsId = registry.createId(
                METRICS_ROOT,
                context.entrySet().stream().map(e -> new BasicTag(e.getKey(), e.getValue())).collect(Collectors.toList())
        );
    }

    @Override
    public Mono<T> apply(Mono<T> source) {
        if (thresholdSteps.isEmpty()) {
            return source;
        }

        Flux<Either<T, Throwable>> fluxAction = source.compose(ReactorExt.either()).flux();

        List<Flux<Either<T, Throwable>>> all = new ArrayList<>();
        all.add(fluxAction);

        thresholdSteps.forEach(thresholdStep -> {
            Flux<Either<T, Throwable>> action = fluxAction.delaySubscription(thresholdStep.getLeft(), scheduler);
            for (int i = 0; i < thresholdStep.getRight(); i++) {
                all.add(action);
            }
        });

        return Flux.merge(all).subscribeOn(scheduler).takeUntil(Either::hasValue).collectList().flatMap(results -> {
            if (results.isEmpty()) {
                IllegalStateException error = new IllegalStateException("No result or failure");
                reportErrors(Collections.singletonList(error));
                return Mono.error(error);
            }

            List<Throwable> errors = results.stream().filter(Either::hasError).map(Either::getError).collect(Collectors.toList());

            if (results.size() == errors.size()) {
                reportErrors(errors);
                return Mono.error(new CompositeException(errors));
            }

            T value = CollectionsExt.last(results).getValue();
            reportSuccess(value, errors);
            return Mono.just(value);
        });
    }

    private void reportSuccess(T value, List<Throwable> errors) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] Request succeeded with {} failed attempts: result={}, errors={}",
                    context, errors.size(), value, errors
            );
        }

        registry.counter(metricsId
                .withTag("status", "success")
                .withTag("failedAttempts", "" + errors.size())
        ).increment();
    }

    private void reportErrors(List<Throwable> errors) {
        if (logger.isDebugEnabled()) {
            logger.debug("[{}] Request failed after {} attempts: errors={}", context, errors.size(), errors);
        }

        registry.counter(metricsId
                .withTag("status", "failure")
                .withTag("failedAttempts", "" + errors.size())
        ).increment();
    }

    static <T> ReactorHedgedTransformer<T> newFromThresholds(List<Duration> thresholds, Map<String, String> context, Registry registry, Scheduler scheduler) {
        List<Pair<Duration, Long>> thresholdSteps;

        if (thresholds.isEmpty()) {
            thresholdSteps = Collections.emptyList();
        } else {
            Map<Duration, Long> grouped = thresholds.stream().collect(Collectors.groupingBy(
                    Function.identity(),
                    Collectors.counting()
            ));
            thresholdSteps = grouped.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(e -> Pair.of(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
        }
        return new ReactorHedgedTransformer<>(thresholdSteps, context, registry, scheduler);
    }
}
