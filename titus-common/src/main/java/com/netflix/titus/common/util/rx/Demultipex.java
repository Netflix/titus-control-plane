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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.ExceptionExt;
import rx.Emitter;
import rx.Observable;
import rx.Subscription;

/**
 * See {@link ObservableExt#demultiplex(Observable, int)} for more information.
 */
class Demultipex<T> {

    private final Observable<T> source;
    private final AtomicInteger remainingSubscriptions;
    private final ConcurrentMap<Integer, Emitter<T>> emittersByOutputIdx = new ConcurrentHashMap<>();
    private final List<Observable<T>> outputs;

    private volatile Subscription sourceSubscription;
    private volatile boolean sourceTerminated;

    private Demultipex(Observable<T> source, int outputs) {
        this.source = source;
        this.remainingSubscriptions = new AtomicInteger(outputs);
        this.outputs = Evaluators.evaluateTimes(outputs, this::newOutput);
    }

    private List<Observable<T>> getOutputs() {
        return outputs;
    }

    private Observable<T> newOutput(int index) {
        return Observable.create(emitter -> {
            if (emittersByOutputIdx.putIfAbsent(index, emitter) != null) {
                emitter.onError(new IllegalStateException(String.format("Demultiplex output %s already subscribed to", index)));
                return;
            }

            emitter.setCancellation(this::cancelAll);

            if (remainingSubscriptions.decrementAndGet() == 0) {
                this.sourceSubscription = source.subscribe(this::emitOnNext, this::emitOnError, this::emitOnCompleted);
            }
        }, Emitter.BackpressureMode.NONE);
    }

    private void emitOnNext(T next) {
        try {
            emittersByOutputIdx.values().forEach(emitter -> emitter.onNext(next));
        } catch (Exception e) {
            emitOnError(e);
        }
    }

    private void emitOnError(Throwable e) {
        this.sourceTerminated = true;
        emittersByOutputIdx.values().forEach(emitter -> ExceptionExt.silent(() -> emitter.onError(e)));
    }

    private void emitOnCompleted() {
        this.sourceTerminated = true;
        emittersByOutputIdx.values().forEach(emitter -> ExceptionExt.silent(emitter::onCompleted));
    }

    private void cancelAll() {
        if (!sourceTerminated) {
            ObservableExt.safeUnsubscribe(sourceSubscription);
            emitOnError(new IllegalStateException("Cancelled"));
        }
    }

    public static <T> List<Observable<T>> create(Observable<T> source, int outputs) {
        return new Demultipex<>(source, outputs).getOutputs();
    }
}
