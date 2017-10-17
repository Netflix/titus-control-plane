/*
 * Copyright 2017 Netflix, Inc.
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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import rx.Observable;
import rx.Producer;
import rx.Scheduler;
import rx.Subscriber;
import rx.internal.operators.BackpressureUtils;
import rx.subscriptions.Subscriptions;

class ValueGenerator<T> implements Producer {

    private final Function<Long, T> source;
    private final Subscriber<? super T> subscriber;
    private final Scheduler.Worker worker;

    private final AtomicLong nextIndex = new AtomicLong();
    private final AtomicLong requested = new AtomicLong();


    ValueGenerator(Function<Long, T> source, Subscriber<? super T> subscriber, Scheduler scheduler) {
        this.source = source;
        this.subscriber = subscriber;

        this.worker = scheduler.createWorker();
        subscriber.add(Subscriptions.create(worker::unsubscribe));

        worker.schedule(() -> subscriber.setProducer(this));
    }

    @Override
    public void request(long n) {
        if (n > 0) {
            BackpressureUtils.getAndAddRequest(requested, n);
            worker.schedule(this::drain);
        }
    }

    private void drain() {
        while (!subscriber.isUnsubscribed() && requested.get() > 0 && doOne()) {
            requested.decrementAndGet();
        }
    }

    private boolean doOne() {
        try {
            subscriber.onNext(source.apply(nextIndex.getAndIncrement()));
        } catch (Exception e) {
            subscriber.onError(e);
            return false;
        }
        return true;
    }

    static <T> Observable<T> from(Supplier<T> source, Scheduler scheduler) {
        return Observable.unsafeCreate(subscriber -> new ValueGenerator<>(idx -> source.get(), subscriber, scheduler));
    }

    static <T> Observable<T> from(Function<Long, T> source, Scheduler scheduler) {
        return Observable.unsafeCreate(subscriber -> new ValueGenerator<>(source, subscriber, scheduler));
    }
}
