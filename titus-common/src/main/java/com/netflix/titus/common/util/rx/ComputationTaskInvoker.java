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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.functions.Action1;
import rx.subjects.AsyncSubject;

/**
 * A computation task invoker with the following properties:
 * <ul>
 * <li>Each computation is represented as an {@link Observable} emitting one or more elements, that ultimately completes</li>
 * <li>Only one computation runs at a time. Only when it completes another one is started.</li>
 * <li>If a computation is running, and new recompute requests are submitted, they are queued, until the running computation completes</li>
 * <li>For a backlog of computation requests in the queue, a single computation is performed</li>
 * </ul>
 * This invoker is useful if computations mustn't be done in parallel, and its result depends on point in time
 * (give me result created no later than when the request was submitted). The latter property allows for computation
 * result sharing across queued requests.
 */
public class ComputationTaskInvoker<O> {

    private static final Logger logger = LoggerFactory.getLogger(ComputationTaskInvoker.class);

    private final Scheduler.Worker worker;
    private final Observable<O> computation;

    private final BlockingQueue<Observer<? super O>> waitingObservers = new LinkedBlockingDeque<>();
    private final AtomicReference<Observable<Void>> pendingComputation = new AtomicReference<>();

    public ComputationTaskInvoker(Observable<O> computation, Scheduler scheduler) {
        this.worker = scheduler.createWorker();
        this.computation = computation;
    }

    public Observable<O> recompute() {
        return Observable.create(subscriber -> {
            waitingObservers.add(subscriber);
            worker.schedule(this::drain);
        });
    }

    private void drain() {
        if (waitingObservers.isEmpty()) {
            return;
        }

        AsyncSubject<Void> subject = AsyncSubject.create();
        Observable<Void> pending = pendingComputation.get();
        while (pending == null) {
            if (pendingComputation.compareAndSet(null, subject)) {
                pending = subject;
            } else {
                pending = pendingComputation.get();
            }
        }

        if (pending == subject) {
            List<Observer<? super O>> available = new ArrayList<>();
            waitingObservers.drainTo(available);
            computation
                    .doOnTerminate(() -> {
                        pendingComputation.set(null);
                        subject.onCompleted();
                    })
                    .subscribe(
                            next -> doSafely(available, o -> o.onNext(next)),
                            e -> doSafely(available, o -> o.onError(e)),
                            () -> doSafely(available, Observer::onCompleted)
                    );
        } else {
            pending.doOnTerminate(() -> worker.schedule(this::drain)).subscribe();
        }
    }

    private void doSafely(List<Observer<? super O>> observers, Action1<Observer<? super O>> action) {
        observers.forEach(o -> {
            try {
                action.call(o);
            } catch (Throwable e) {
                logger.debug("Observable invocation failure", e);
            }
        });
    }
}
