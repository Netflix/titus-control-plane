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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.base.Preconditions;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.observers.SerializedSubscriber;
import rx.schedulers.Schedulers;

/**
 * Executes a set of actions in order.
 */
public class SerializedTaskInvoker {

    private static final Exception INVOKER_SHUTDOWN = new Exception(SerializedTaskInvoker.class.getSimpleName() + " is already shutdown");

    private final Scheduler.Worker worker;
    /* Visible for testing */ final Queue<Subscriber> activeSubscribers = new ConcurrentLinkedQueue<>();

    /**
     * {@link Scheduler} must run tasks on single thread. {@link Schedulers#computation()} should be best choice.
     */
    public SerializedTaskInvoker(Scheduler scheduler) {
        this.worker = scheduler.createWorker();
    }

    public void shutdown() {
        worker.unsubscribe();
        terminateActiveSubscribers();
    }

    public <T> Observable<T> submit(Action1<Subscriber<T>> action) {
        return Observable.create(subscriber -> {
            if (worker.isUnsubscribed()) {
                subscriber.onError(INVOKER_SHUTDOWN);
                return;
            }

            // Action updates below can be run concurrently with shutdown logic.
            SerializedSubscriber<T> serializedSubscriber = new SerializedSubscriber<>(subscriber);
            activeSubscribers.add(serializedSubscriber);

            if (worker.isUnsubscribed()) {
                terminateActiveSubscribers();
            } else {
                worker.schedule(() -> {
                    try {
                        action.call(serializedSubscriber);
                        if (subscriber.isUnsubscribed()) {
                            serializedSubscriber.onCompleted();
                        }
                    } catch (Throwable e) {
                        serializedSubscriber.onError(e);
                    } finally {
                        Subscriber lastSubscriber = activeSubscribers.poll();
                        // Check invariants only if shutdown has not been called.
                        if (!worker.isUnsubscribed()) {
                            Preconditions.checkArgument(
                                    lastSubscriber == serializedSubscriber,
                                    "Invariant violation. Subscriber polled from queue different from the current one"
                            );
                        }
                    }
                });
            }
        });
    }

    public Observable<Void> submit(Action0 action) {
        return submit(subscriber -> {
            action.call();
            subscriber.onCompleted();
        });
    }

    private void terminateActiveSubscribers() {
        while (!activeSubscribers.isEmpty()) {
            Subscriber subscriber = activeSubscribers.poll();
            if (subscriber != null) {
                subscriber.onError(INVOKER_SHUTDOWN);
            }
        }
    }
}
