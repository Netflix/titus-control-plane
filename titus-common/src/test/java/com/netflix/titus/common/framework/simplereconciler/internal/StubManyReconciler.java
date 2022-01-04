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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.ManyReconciler;
import com.netflix.titus.common.framework.simplereconciler.SimpleReconcilerEvent;
import com.netflix.titus.common.util.collections.index.IndexSet;
import com.netflix.titus.common.util.collections.index.Indexes;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

class StubManyReconciler<DATA> implements ManyReconciler<DATA> {

    private final ConcurrentMap<String, DATA> state = new ConcurrentHashMap<>();

    private final AtomicLong transactionIds = new AtomicLong();

    private final Sinks.Many<List<SimpleReconcilerEvent<DATA>>> eventSink = Sinks.many().multicast().directAllOrNothing();
    private final List<List<SimpleReconcilerEvent<DATA>>> eventsCollector = new CopyOnWriteArrayList<>();

    @Override
    public Mono<Void> add(String id, DATA initial) {
        return Mono.fromRunnable(() -> {
            Preconditions.checkState(!state.containsKey(id));
            state.put(id, initial);
            emitEvent(SimpleReconcilerEvent.Kind.Added, id, initial);
        });
    }

    @Override
    public Mono<Void> remove(String id) {
        return Mono.fromRunnable(() -> {
            Preconditions.checkState(state.containsKey(id));
            DATA value = state.remove(id);
            emitEvent(SimpleReconcilerEvent.Kind.Removed, id, value);
        });
    }

    @Override
    public Mono<Void> close() {
        return Mono.empty();
    }

    @Override
    public Map<String, DATA> getAll() {
        return new HashMap<>(state);
    }

    @Override
    public IndexSet<String, DATA> getIndexSet() {
        return Indexes.empty();
    }

    @Override
    public Optional<DATA> findById(String id) {
        return Optional.ofNullable(state.get(id));
    }

    @Override
    public int size() {
        return state.size();
    }

    @Override
    public Mono<DATA> apply(String id, Function<DATA, Mono<DATA>> action) {
        return Mono.defer(() -> {
            Preconditions.checkState(state.containsKey(id));
            return action.apply(state.get(id)).doOnNext(newValue -> {
                state.put(id, newValue);
                emitEvent(SimpleReconcilerEvent.Kind.Updated, id, newValue);
            });
        });
    }

    @Override
    public Flux<List<SimpleReconcilerEvent<DATA>>> changes(String clientId) {
        List<SimpleReconcilerEvent<DATA>> snapshot = new ArrayList<>();
        state.forEach((id, value) -> snapshot.add(new SimpleReconcilerEvent<>(SimpleReconcilerEvent.Kind.Added, id, value, transactionIds.getAndIncrement())));
        return Flux.just(snapshot).concatWith(eventSink.asFlux());
    }

    private void emitEvent(SimpleReconcilerEvent.Kind kind, String id, DATA value) {
        List<SimpleReconcilerEvent<DATA>> events = Collections.singletonList(
                new SimpleReconcilerEvent<>(kind, id, value, transactionIds.getAndIncrement())
        );
        eventsCollector.add(events);
        eventSink.emitNext(events, Sinks.EmitFailureHandler.FAIL_FAST);
    }
}
