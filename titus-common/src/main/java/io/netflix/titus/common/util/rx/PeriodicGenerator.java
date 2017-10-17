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

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import rx.Observable;
import rx.Producer;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;
import rx.subscriptions.Subscriptions;

/**
 * See {@link ObservableExt#generatorFrom(Supplier)} (Observable, long, long, TimeUnit, Scheduler)}.
 */
class PeriodicGenerator<T> {

    private final Observable<T> sourceObservable;
    private final Scheduler scheduler;
    private final long initialDelayMs;
    private final long intervalMs;

    PeriodicGenerator(Observable<T> sourceObservable,
                      long initialDelay,
                      long interval,
                      TimeUnit timeUnit,
                      Scheduler scheduler) {
        this.sourceObservable = sourceObservable;
        this.scheduler = scheduler;
        this.initialDelayMs = timeUnit.toMillis(initialDelay);
        this.intervalMs = timeUnit.toMillis(interval);
    }

    Observable<List<T>> doMany() {
        return Observable.unsafeCreate(subscriber -> {
            Subscription subscription = ObservableExt.generatorFrom(index -> doOne(index == 0), scheduler)
                    .flatMap(d -> d, 1)
                    .subscribe(new Subscriber<List<T>>() {
                                   private Producer producer;

                                   @Override
                                   public void setProducer(Producer p) {
                                       super.setProducer(p);
                                       this.producer = p;
                                       p.request(1);
                                   }

                                   @Override
                                   public void onNext(List<T> item) {
                                       subscriber.onNext(item);
                                       producer.request(1);

                                   }

                                   @Override
                                   public void onCompleted() {
                                       subscriber.onCompleted();
                                   }

                                   @Override
                                   public void onError(Throwable e) {
                                       subscriber.onError(e);
                                   }
                               }
                    );
            subscriber.add(Subscriptions.create(subscription::unsubscribe));
        });
    }

    private Observable<List<T>> doOne(boolean firstEmit) {
        long delayMs = firstEmit ? initialDelayMs : intervalMs;
        return Observable.timer(delayMs, TimeUnit.MILLISECONDS, scheduler).flatMap(tick -> sourceObservable).toList();
    }

    static <T> Observable<List<T>> from(Observable<T> sourceObservable,
                                        long initialDelay,
                                        long interval,
                                        TimeUnit timeUnit,
                                        Scheduler scheduler) {
        return new PeriodicGenerator<>(sourceObservable, initialDelay, interval, timeUnit, scheduler).doMany();
    }
}
