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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

class EventEmitterTransformer<K, T, E> implements Function<Flux<List<T>>, Publisher<E>> {

    private final Function<T, K> keyFun;
    private final BiFunction<T, T, Boolean> valueComparator;
    private final Function<T, E> valueAddedEventMapper;
    private final Function<T, E> valueRemovedEventMapper;
    private final E snapshotEndEvent;

    EventEmitterTransformer(
            Function<T, K> keyFun,
            BiFunction<T, T, Boolean> valueComparator,
            Function<T, E> valueAddedEventMapper,
            Function<T, E> valueRemovedEventMapper,
            E snapshotEndEvent) {
        this.keyFun = keyFun;
        this.valueComparator = valueComparator;
        this.valueAddedEventMapper = valueAddedEventMapper;
        this.valueRemovedEventMapper = valueRemovedEventMapper;
        this.snapshotEndEvent = snapshotEndEvent;
    }

    @Override
    public Publisher<E> apply(Flux<List<T>> source) {
        return Flux.defer(() -> {
            AtomicBoolean snapshotSent = new AtomicBoolean();
            AtomicReference<Map<K, T>> valueMapRef = new AtomicReference<>(Collections.emptyMap());
            return source.flatMapIterable(newSnapshot -> {
                Map<K, T> previousValueMap = valueMapRef.get();
                Map<K, T> newValueMap = toValueMap(newSnapshot);

                List<E> events = new ArrayList<>();

                // Updates
                newValueMap.forEach((key, value) -> {
                    T previousValue = previousValueMap.get(key);
                    if (previousValue == null || !valueComparator.apply(value, previousValue)) {
                        events.add(valueAddedEventMapper.apply(value));
                    }
                });

                // Removes
                previousValueMap.forEach((key, value) -> {
                    if (!newValueMap.containsKey(key)) {
                        events.add(valueRemovedEventMapper.apply(value));
                    }
                });

                valueMapRef.set(newValueMap);

                if (!snapshotSent.getAndSet(true)) {
                    events.add(snapshotEndEvent);
                }

                return events;
            });
        });
    }

    private Map<K, T> toValueMap(List<T> values) {
        if (values.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<K, T> valueMap = new HashMap<>();
        values.forEach(value -> valueMap.put(keyFun.apply(value), value));
        return valueMap;
    }
}
