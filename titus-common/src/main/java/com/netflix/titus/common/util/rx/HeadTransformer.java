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
import java.util.function.Supplier;

import rx.Observable;
import rx.Subscription;
import rx.observers.SerializedSubscriber;
import rx.subscriptions.Subscriptions;

/**
 * An operator that combines snapshots state with hot updates. To prevent loss of
 * any update for a given snapshot, the hot subscriber is subscribed first, and its
 * values are buffered until the snapshot state is stream to the subscriber.
 */
class HeadTransformer<T> implements Observable.Transformer<T, T> {

    private static final Object END_OF_STREAM_MARKER = new Object();

    private enum State {Head, HeadDone, Buffer}

    private final Supplier<Collection<T>> headValueSupplier;

    public HeadTransformer(Supplier<Collection<T>> headValueSupplier) {
        this.headValueSupplier = headValueSupplier;
    }

    @Override
    public Observable<T> call(Observable<T> hotObservable) {
        return Observable.unsafeCreate(subscriber -> {
            SerializedSubscriber<T> serialized = new SerializedSubscriber<>(subscriber);
            Queue<T> buffer = new ConcurrentLinkedDeque<>();
            AtomicReference<State> state = new AtomicReference<>(State.Head);

            Subscription hotSubscription = null;
            try {
                // Subscribe before we evaluate head values
                hotSubscription = hotObservable.subscribe(
                        next -> {
                            if (state.get() == State.Buffer || state.compareAndSet(State.HeadDone, State.Buffer)) {
                                drainBuffer(serialized, buffer);
                                serialized.onNext(next);
                            } else {
                                buffer.add(next);
                            }
                        },
                        serialized::onError,
                        () -> {
                            if (state.get() == State.Buffer || state.compareAndSet(State.HeadDone, State.Buffer)) {
                                drainBuffer(serialized, buffer);
                                serialized.onCompleted();
                            } else {
                                buffer.add((T) END_OF_STREAM_MARKER);
                            }
                        }
                );

                Subscription finalHotSubscription = hotSubscription;
                serialized.add(Subscriptions.create(finalHotSubscription::unsubscribe));

                // Stream head
                if (serialized.isUnsubscribed()) {
                    return;
                }

                for (T next : headValueSupplier.get()) {
                    if (serialized.isUnsubscribed()) {
                        return;
                    }
                    serialized.onNext(next);
                }

                do {
                    boolean endOfStream = !drainBuffer(serialized, buffer);
                    if (endOfStream) {
                        serialized.onCompleted();
                        break;
                    }
                    state.set(State.HeadDone);
                }
                while (!serialized.isUnsubscribed() && !buffer.isEmpty() && state.compareAndSet(State.HeadDone, State.Head));
            } catch (Throwable e) {
                serialized.onError(e);
                if (hotSubscription != null) {
                    hotSubscription.unsubscribe();
                }
            }
        });
    }

    private boolean drainBuffer(SerializedSubscriber<T> serialized, Queue<T> buffer) {
        for (T next = buffer.poll(); !serialized.isUnsubscribed() && next != null; next = buffer.poll()) {
            if (next == END_OF_STREAM_MARKER) {
                return false;
            }
            serialized.onNext(next);
        }
        return true;
    }
}
