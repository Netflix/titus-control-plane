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

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * An operator that combines snapshots state with hot updates. To prevent loss of
 * any update for a given snapshot, the hot subscriber is subscribed first, and its
 * values are buffered until the snapshot state is streamed to the subscriber.
 */
class ReactorHeadTransformer<T> implements Function<Flux<T>, Publisher<T>> {

    private static final Object END_OF_STREAM_MARKER = new Object();

    private enum State {Head, HeadDone, Buffer}

    private final Supplier<Collection<T>> headValueSupplier;

    ReactorHeadTransformer(Supplier<Collection<T>> headValueSupplier) {
        this.headValueSupplier = headValueSupplier;
    }

    @Override
    public Publisher<T> apply(Flux<T> tFlux) {
        return Flux.create(emitter -> {
            Queue<Object> buffer = new ConcurrentLinkedDeque<>();
            AtomicReference<State> state = new AtomicReference<>(State.Head);

            Disposable hotSubscription = null;
            try {
                // Subscribe before we evaluate head values
                hotSubscription = tFlux.subscribe(
                        next -> {
                            if (state.get() == State.Buffer || state.compareAndSet(State.HeadDone, State.Buffer)) {
                                drainBuffer(emitter, buffer);
                                emitter.next(next);
                            } else {
                                buffer.add(next);
                            }
                        },
                        e -> {
                            if (state.get() == State.Buffer || state.compareAndSet(State.HeadDone, State.Buffer)) {
                                drainBuffer(emitter, buffer);
                                emitter.error(e);
                            } else {
                                buffer.add(new ErrorWrapper(e));
                            }
                        },
                        () -> {
                            if (state.get() == State.Buffer || state.compareAndSet(State.HeadDone, State.Buffer)) {
                                drainBuffer(emitter, buffer);
                                emitter.complete();
                            } else {
                                buffer.add(END_OF_STREAM_MARKER);
                            }
                        }
                );
                emitter.onDispose(hotSubscription);

                // Stream head
                if (emitter.isCancelled()) {
                    return;
                }

                for (T next : headValueSupplier.get()) {
                    if (emitter.isCancelled()) {
                        return;
                    }
                    emitter.next(next);
                }

                do {
                    boolean endOfStream = !drainBuffer(emitter, buffer);
                    if (endOfStream) {
                        emitter.complete();
                        break;
                    }
                    state.set(State.HeadDone);
                }
                while (!emitter.isCancelled() && !buffer.isEmpty() && state.compareAndSet(State.HeadDone, State.Head));
            } catch (Throwable e) {
                emitter.error(e);
                ReactorExt.safeDispose(hotSubscription);
            }
        });
    }

    private boolean drainBuffer(FluxSink<T> emitter, Queue<Object> buffer) {
        for (Object next = buffer.poll(); !emitter.isCancelled() && next != null; next = buffer.poll()) {
            if (next == END_OF_STREAM_MARKER) {
                return false;
            }
            if (next instanceof ErrorWrapper) {
                emitter.error(((ErrorWrapper) next).cause);
                return false;
            }
            emitter.next((T) next);
        }
        return true;
    }

    private static class ErrorWrapper {

        private final Throwable cause;

        private ErrorWrapper(Throwable cause) {
            this.cause = cause;
        }
    }
}
