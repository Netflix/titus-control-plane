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

package com.netflix.titus.common.grpc;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.annotation.Nullable;

import rx.Emitter;
import rx.Observer;
import rx.Subscription;
import rx.functions.Cancellable;
import rx.internal.subscriptions.CancellableSubscription;
import rx.observers.SerializedObserver;
import rx.subscriptions.Subscriptions;

/**
 * Emitter implementation that accumulates {@link Subscription subscriptions} and {@link Cancellable cancellations}
 * instead of replacing the existing one(s).
 * <p>
 * Current subscriptions and cancellations can be reset by passing <tt>null</tt> to {@link EmitterWithMultipleSubscriptions#setCancellation(Cancellable)}
 * and {@link EmitterWithMultipleSubscriptions#setSubscription(Subscription)}.
 */
public class EmitterWithMultipleSubscriptions<T> implements Emitter<T> {
    // only kept for requested() delegation
    private final Emitter<T> actual;

    private final Observer<T> delegate;
    private final Collection<Subscription> subscriptions = new ConcurrentLinkedQueue<>();

    public EmitterWithMultipleSubscriptions(Emitter<T> delegate) {
        this.actual = delegate;
        this.delegate = new SerializedObserver<>(delegate);
        Subscription composedSubscription = Subscriptions.create(() -> subscriptions.forEach(Subscription::unsubscribe));
        actual.setSubscription(composedSubscription);
    }

    /**
     * Add a new subscription while keeping the previous one(s). Note that this is different from the original contract
     * defined by {@link Emitter#setSubscription(Subscription)}.
     *
     * @param s will reset all subscriptions and cancellations when <tt>null</tt>
     */
    @Override
    public void setSubscription(@Nullable Subscription s) {
        if (s == null) {
            subscriptions.clear();
            return;
        }
        subscriptions.add(s);
    }

    /**
     * Add a new cancellation while keeping the previous one(s). Note that this is different from the original contract
     * defined by {@link Emitter#setCancellation(Cancellable)}.
     *
     * @param c will reset all cancellations and subscriptions when <tt>null</tt>
     */
    @Override
    public final void setCancellation(@Nullable Cancellable c) {
        if (c == null) {
            subscriptions.clear();
            return;
        }
        subscriptions.add(new CancellableSubscription(c));
    }

    @Override
    public long requested() {
        return actual.requested();
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
        delegate.onError(e);
    }

    @Override
    public void onNext(T t) {
        delegate.onNext(t);
    }
}
