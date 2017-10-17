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

package io.netflix.titus.testkit.rx;

import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;

/**
 * A wrapper for an observable instance recording its subscriptions/notifications.
 */
public class ObservableRecorder<T> {

    private final Observable<T> trackingObservable;

    private AtomicInteger startedSubscriptionsCounter = new AtomicInteger();
    private AtomicInteger finishedSubscriptionsCounter = new AtomicInteger();

    private ObservableRecorder(Observable<T> observable) {
        this.trackingObservable = Observable.create(subscriber -> {
            startedSubscriptionsCounter.incrementAndGet();
            observable.doOnTerminate(() -> finishedSubscriptionsCounter.incrementAndGet()).subscribe(subscriber);
        });
    }

    public Observable<T> getObservable() {
        return trackingObservable;
    }

    public int numberOfFinishedSubscriptions() {
        return finishedSubscriptionsCounter.get();
    }

    public static <T> ObservableRecorder<T> newRecorder(Observable<T> observable) {
        return new ObservableRecorder<>(observable);
    }
}
