/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.rx.ReactorExt;
import com.netflix.titus.common.util.tuple.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.MonoSink;

class SingleTransaction<DATA> implements Transaction<DATA> {

    private static final Logger logger = LoggerFactory.getLogger(SingleTransaction.class);

    private final ChangeActionHolder<DATA> actionHolder;
    private final Disposable subscription;

    private final AtomicReference<State> stateRef = new AtomicReference<>(State.Running);
    private final AtomicReference<Function<DATA, DATA>> updateFunRef = new AtomicReference<>();
    private final AtomicReference<Throwable> errorRef = new AtomicReference<>();
    private final AtomicReference<DATA> resultRef = new AtomicReference<>();

    SingleTransaction(ChangeActionHolder<DATA> actionHolder) {
        this.subscription = actionHolder.getAction()
                .doFinally(signal -> stateRef.compareAndSet(State.Running, State.Cancelled))
                .subscribe(
                        newValue -> {
                            updateFunRef.set(newValue);
                            stateRef.set(State.ResultReady);
                        },
                        error -> {
                            errorRef.set(error);
                            stateRef.set(State.ResultReady);
                            if (actionHolder.getSubscriberSink() == null) {
                                logger.warn("Reconciliation action failure", error);
                            }
                        },
                        () -> stateRef.set(State.ResultReady)
                );
        actionHolder.addCancelCallback(subscription);
        this.actionHolder = actionHolder;
    }

    @Override
    public void close() {
        ReactorExt.safeDispose(subscription);
        if (actionHolder.getSubscriberSink() != null) {
            try {
                actionHolder.getSubscriberSink().error(new IllegalStateException("Reconciliation engine closed"));
            } catch (Exception ignore) {
            }
        } else {
            logger.warn("Cancelling transaction {}. Reconciliation engine closed", actionHolder.getTransactionId());
        }
    }

    @Override
    public State getState() {
        return stateRef.get();
    }

    @Override
    public Either<DATA, Throwable> applyDataChanges(DATA current) {
        Preconditions.checkState(stateRef.get() == State.ResultReady, "Result no available");

        if (errorRef.get() != null) {
            return Either.ofError(errorRef.get());
        }
        if (updateFunRef.get() == null) {
            return Either.ofValue(current);
        }
        try {
            DATA result = updateFunRef.get().apply(current);
            resultRef.set(result);
            return Either.ofValue(result);
        } catch (Exception e) {
            errorRef.set(e);
            if (actionHolder.getSubscriberSink() == null) {
                logger.warn("Reconciliation data update failure", e);
            }
            return Either.ofError(e);
        }
    }

    @Override
    public void complete() {
        Preconditions.checkState(stateRef.get() != State.SubscriberNotified, "Subscriber already notified");
        Preconditions.checkState(stateRef.get() == State.ResultReady, "Result no available");

        MonoSink<DATA> sink = actionHolder.getSubscriberSink();
        if (sink == null) {
            stateRef.set(State.SubscriberNotified);
            return;
        }

        try {
            if (errorRef.get() != null) {
                try {
                    sink.error(errorRef.get());
                } catch (Exception e) {
                    logger.warn("Unexpected error", e);
                }
            } else if (resultRef.get() != null) {
                try {
                    sink.success(resultRef.get());
                } catch (Exception e) {
                    logger.warn("Unexpected error", e);
                }
            } else {
                try {
                    sink.success();
                } catch (Exception e) {
                    logger.warn("Unexpected error", e);
                }
            }
        } finally {
            stateRef.set(State.SubscriberNotified);
        }
    }
}
