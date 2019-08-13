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

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.tuple.Either;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.MonoSink;

class FailedTransaction<DATA> implements Transaction<DATA> {

    private static final Logger logger = LoggerFactory.getLogger(FailedTransaction.class);

    private final ChangeActionHolder actionHolder;
    private final Throwable error;
    private final AtomicReference<State> stateRef;

    FailedTransaction(ChangeActionHolder actionHolder, Throwable error) {
        this.actionHolder = actionHolder;
        this.error = error;
        this.stateRef = new AtomicReference<>(State.ResultReady);
    }

    @Override
    public void close() {
    }

    @Override
    public State getState() {
        return stateRef.get();
    }

    @Override
    public Either<DATA, Throwable> applyDataChanges(DATA current) {
        return Either.ofError(error);
    }

    @Override
    public void complete() {
        Preconditions.checkState(stateRef.get() != State.SubscriberNotified, "Subscriber already notified");
        Preconditions.checkState(stateRef.get() == State.ResultReady, "Result no available");

        MonoSink<DATA> sink = actionHolder.getSubscriberSink();
        if (sink != null) {
            try {
                sink.error(error);
            } catch (Exception e) {
                logger.warn("Unexpected error", e);
            }
        }
    }
}
