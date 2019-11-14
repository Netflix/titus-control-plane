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

package com.netflix.titus.common.framework.simplereconciler.internal.transaction;

import java.util.Objects;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

public class TransactionStatus<DATA> {

    public enum State {
        Started,
        ResultReady,
        Completed,
        Failed,
        Cancelled;
    }

    private final State state;
    private final Function<DATA, DATA> resultEvaluator;
    private final DATA result;
    private final Throwable error;

    public TransactionStatus(State state,
                             @Nullable Function<DATA, DATA> resultEvaluator,
                             @Nullable DATA result,
                             @Nullable Throwable error) {
        this.state = state;
        this.resultEvaluator = resultEvaluator;
        this.result = result;
        this.error = error;
    }

    public State getState() {
        return state;
    }

    @Nullable
    public DATA getResult() {
        return result;
    }

    public Throwable getError() {
        return error;
    }

    public TransactionStatus<DATA> resultReady(Function<DATA, DATA> resultEvaluator) {
        Preconditions.checkState(state == State.Started, "Not in Started state: %s", state);
        return new TransactionStatus<>(State.ResultReady, resultEvaluator, null, null);
    }

    public TransactionStatus<DATA> complete(DATA current) {
        Preconditions.checkState(state == State.ResultReady, "Not in ResultReady state: %s", state);
        try {
            return new TransactionStatus<>(
                    State.Completed,
                    null,
                    resultEvaluator.apply(current),
                    null
            );
        } catch (Exception e) {
            return failed(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionStatus<?> that = (TransactionStatus<?>) o;
        return state == that.state &&
                Objects.equals(resultEvaluator, that.resultEvaluator) &&
                Objects.equals(result, that.result) &&
                Objects.equals(error, that.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, resultEvaluator, result, error);
    }

    @Override
    public String toString() {
        switch (state) {
            case ResultReady:
            case Completed:
                return "TransactionStatus{" +
                        "state=" + state +
                        ", result=" + result +
                        '}';
            case Failed:
                return "TransactionStatus{" +
                        "state=" + state +
                        ", error=" + error +
                        '}';
            case Started:
            case Cancelled:
            default:
                return "TransactionStatus{" +
                        "state=" + state +
                        '}';
        }
    }

    public static <DATA> TransactionStatus<DATA> started() {
        return new TransactionStatus<>(State.Started, null, null, null);
    }

    public static <DATA> TransactionStatus<DATA> failed(Throwable error) {
        Preconditions.checkNotNull(error, "Error is null");
        return new TransactionStatus<>(State.Failed, null, null, error);
    }

    public static <DATA> TransactionStatus<DATA> cancelled() {
        return new TransactionStatus<>(State.Cancelled, null, null, null);
    }
}
