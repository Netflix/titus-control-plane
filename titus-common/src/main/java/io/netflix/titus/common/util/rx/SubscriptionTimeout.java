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

package io.netflix.titus.common.util.rx;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import rx.Observable;
import rx.Producer;
import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.Subscription;
import rx.internal.producers.ProducerArbiter;
import rx.observers.SerializedSubscriber;

/**
 * Apply a timeout for an entire subscription of a source {@link Observable}. Downstream subscribers will receive an
 * {@link Subscriber#onError(Throwable) onError} event with a {@link TimeoutException} if a timeout happens before the
 * source {@link Observable} completes.
 * <p>
 * The implementation is inspired by {@link rx.internal.operators.OnSubscribeTimeoutTimedWithFallback}, but it applies a
 * timeout to the entire subscription, rather than between each {@link Subscriber#onNext(Object) onNext emission}.
 * <p>
 * All onNext, onError and onCompleted calls from the source {@link Observable} will be serialized to prevent any races,
 * otherwise the {@link TimeoutException} generated internally would race with external events.
 *
 * @see rx.internal.operators.OnSubscribeTimeoutTimedWithFallback
 * @see Observable#timeout(long, TimeUnit, Scheduler)
 */
class SubscriptionTimeout<T> implements Observable.Operator<T, T> {
    private final Supplier<Long> timeout;
    private final TimeUnit unit;
    private final Scheduler scheduler;

    SubscriptionTimeout(Supplier<Long> timeout, TimeUnit unit, Scheduler scheduler) {
        this.timeout = timeout;
        this.unit = unit;
        this.scheduler = scheduler;
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super T> downstream) {
        TimeoutSubscriber<T> upstream = new TimeoutSubscriber<T>(downstream, timeout.get(), unit);
        downstream.add(upstream);
        downstream.setProducer(upstream.arbiter);

        // prevent all races and serialize onNext, onError, and onCompleted calls
        final Worker worker = scheduler.createWorker();
        final SerializedSubscriber<T> safeUpstream = new SerializedSubscriber<>(upstream, true);
        upstream.add(worker);

        Subscription task = worker.schedule(() -> safeUpstream.onError(new TimeoutException()), timeout.get(), unit);
        upstream.add(task);

        return safeUpstream;
    }

    private static final class TimeoutSubscriber<T> extends Subscriber<T> {
        private final Subscriber<? super T> actual;
        private final ProducerArbiter arbiter;

        private TimeoutSubscriber(Subscriber<? super T> actual, long timeout, TimeUnit unit) {
            this.actual = actual;
            this.arbiter = new ProducerArbiter();
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable e) {
            unsubscribe();
            actual.onError(e);
        }

        @Override
        public void onCompleted() {
            unsubscribe();
            actual.onCompleted();
        }

        @Override
        public void setProducer(Producer p) {
            arbiter.setProducer(p);
        }
    }
}
