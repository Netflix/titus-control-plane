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

import com.netflix.titus.common.util.tuple.Either;

class EmptyTransaction implements Transaction<Object> {

    private static final Transaction<Object> INSTANCE = new EmptyTransaction();

    @Override
    public void close() {
    }

    @Override
    public State getState() {
        return State.SubscriberNotified;
    }

    @Override
    public Either<Object, Throwable> applyDataChanges(Object current) {
        throw new IllegalStateException("Transaction already completed");
    }

    @Override
    public void complete() {
        throw new IllegalStateException("Transaction already completed");
    }

    public static <DATA> Transaction<DATA> empty() {
        return (Transaction<DATA>) INSTANCE;
    }
}
