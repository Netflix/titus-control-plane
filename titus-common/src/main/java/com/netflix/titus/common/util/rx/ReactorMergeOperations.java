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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;

class ReactorMergeOperations {

    public static <K> Mono<Map<K, Optional<Throwable>>> merge(Map<K, Mono<Void>> monos, int concurrencyLimit, Scheduler scheduler) {
        List<Flux<Pair<K, Optional<Throwable>>>> m2 = new ArrayList<>();
        monos.forEach((key, mono) -> {

            Flux<Pair<K, Optional<Throwable>>> x = mono.toProcessor().ignoreElement().materialize().map(result -> {
                        Optional<Throwable> error = result.getType() == SignalType.ON_ERROR
                                ? Optional.of(result.getThrowable())
                                : Optional.empty();
                        return Pair.of(key, error);
                    }
            ).flux();
            m2.add(x);
        });

        return Flux.merge(Flux.fromIterable(m2), concurrencyLimit)
                .subscribeOn(scheduler)
                .collectList()
                .map(list -> list.stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight)));
    }
}
