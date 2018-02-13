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
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Producer;
import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.Subscription;
import rx.internal.producers.ProducerArbiter;
import rx.plugins.RxJavaHooks;

/**
 * Apply a timeout for an entire subscription of a source {@link Observable}. Downstream subscribers will receive an
 * {@link Subscriber#onError(Throwable) onError} event with a {@link TimeoutException} if a timeout happens before the
 * source {@link Observable} completes.
 * <p>
 * The implementation is inspired by {@link rx.internal.operators.OnSubscribeTimeoutTimedWithFallback}, but it applies a
 * timeout to the entire subscription, rather than between each {@link Subscriber#onNext(Object) onNext emission}.
 *
 * @see rx.internal.operators.OnSubscribeTimeoutTimedWithFallback
 * @see Observable#timeout(long, TimeUnit, Scheduler)
 */
class SubscriptionTimeout<T> implements Observable.Operator<T, T> {
    private static final Logger logger = LoggerFactory.getLogger(SubscriptionTimeout.class);

    private final long timeout;
    private final TimeUnit unit;
    private final Scheduler scheduler;

    SubscriptionTimeout(long timeout, TimeUnit unit, Scheduler scheduler) {
        this.timeout = timeout;
        this.unit = unit;
        this.scheduler = scheduler;
    }

    @Override
    public Subscriber<? super T> call(Subscriber<? super T> downstream) {
        TimeoutSubscriber<T> upstream = new TimeoutSubscriber<T>(downstream, timeout, unit, scheduler.createWorker());
        downstream.add(upstream);
        downstream.setProducer(upstream.arbiter);
        return upstream;
    }

    private static final class TimeoutSubscriber<T> extends Subscriber<T> {
        private final Subscriber<? super T> actual;
        private final ProducerArbiter arbiter;
        private final AtomicBoolean terminated = new AtomicBoolean(false);

        private TimeoutSubscriber(Subscriber<? super T> actual, long timeout, TimeUnit unit, Worker worker) {
            this.actual = actual;
            this.arbiter = new ProducerArbiter();
            Subscription task = worker.schedule(this::onTimeout, timeout, unit);
            this.add(worker);
            this.add(task);
        }

        private void onTimeout() {
            if (!terminated.compareAndSet(false, true)) {
                logger.error("onTimeout after a terminal event");
                return;
            }
            unsubscribe();
            actual.onError(new TimeoutException());
        }

        @Override
        public void onNext(T t) {
            if (terminated.get()) {
                logger.error("onNext after a terminal event");
                return;
            }
            // there is a small chance of a race here, but we accept it to avoid full synchronization (mutex or
            // serialization through an event loop).
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable e) {
            if (!terminated.compareAndSet(false, true)) {
                RxJavaHooks.onError(e);
                return;
            }
            unsubscribe();
            actual.onError(e);
        }

        @Override
        public void onCompleted() {
            if (!terminated.compareAndSet(false, true)) {
                logger.error("onCompleted after a terminal event");
                return;
            }
            unsubscribe();
            actual.onCompleted();
        }

        @Override
        public void setProducer(Producer p) {
            arbiter.setProducer(p);
        }
    }
}
