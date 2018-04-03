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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.subscriptions.Subscriptions;

class FutureOnSubscribe<T> implements Observable.OnSubscribe<T> {

    static final long INITIAL_DELAY_MS = 1;
    static final long MAX_DELAY_MS = 1000;

    private final Future<T> future;
    private final Scheduler scheduler;

    private final AtomicBoolean isSubscribed = new AtomicBoolean();

    FutureOnSubscribe(Future<T> future, Scheduler scheduler) {
        this.future = future;
        this.scheduler = scheduler;
    }

    @Override
    public void call(Subscriber<? super T> subscriber) {
        if (isSubscribed.getAndSet(true)) {
            subscriber.onError(new IllegalStateException("Only one subscription allowed in future wrapper"));
            return;
        }
        new FutureObserver(subscriber);
    }

    private class FutureObserver {
        private final Subscriber<? super T> subscriber;

        private final Scheduler.Worker worker;

        FutureObserver(Subscriber<? super T> subscriber) {
            this.subscriber = subscriber;
            this.worker = scheduler.createWorker();

            subscriber.add(Subscriptions.create(() -> {
                future.cancel(true);
                worker.unsubscribe();
            }));
            doSchedule(INITIAL_DELAY_MS);
        }

        private void doSchedule(long delayMs) {
            if (completeIfDone()) {
                worker.unsubscribe();
            }
            worker.schedule(() -> doSchedule(Math.min(2 * delayMs, MAX_DELAY_MS)), delayMs, TimeUnit.MILLISECONDS);
        }

        private boolean completeIfDone() {
            if (subscriber.isUnsubscribed()) {
                return true;
            }
            if (!future.isDone()) {
                return false;
            }
            if (future.isCancelled()) {
                subscriber.onError(new IllegalStateException("future is cancelled"));
                return true;
            }
            try {
                T value = future.get();
                subscriber.onNext(value);
                subscriber.onCompleted();
            } catch (Exception e) {
                subscriber.onError(e);
            }
            return true;
        }
    }
}
