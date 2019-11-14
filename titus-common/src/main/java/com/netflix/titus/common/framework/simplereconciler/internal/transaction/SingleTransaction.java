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

import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.framework.simplereconciler.internal.ChangeActionHolder;
import com.netflix.titus.common.util.rx.ReactorExt;
import reactor.core.Disposable;

public class SingleTransaction<DATA> implements Transaction<DATA> {

    private final ChangeActionHolder<DATA> actionHolder;
    private final Disposable subscription;

    private volatile TransactionStatus<DATA> transactionStatus;

    public SingleTransaction(DATA current, ChangeActionHolder<DATA> actionHolder) {
        this.transactionStatus = TransactionStatus.started();
        this.subscription = actionHolder.getAction().apply(current)
                .doOnCancel(() -> transitionIfRunning(TransactionStatus::cancelled))
                .subscribe(
                        newValue -> transitionIfRunning(() -> transactionStatus.resultReady(newValue)),
                        error -> transitionIfRunning(() -> TransactionStatus.failed(error)),
                        () -> transitionIfRunning(() -> transactionStatus.resultReady(Function.identity()))
                );
        actionHolder.addCancelCallback(subscription);
        this.actionHolder = actionHolder;
    }

    @Override
    public void cancel() {
        ReactorExt.safeDispose(subscription);
    }

    @Override
    public ChangeActionHolder<DATA> getActionHolder() {
        return actionHolder;
    }

    @Override
    public TransactionStatus<DATA> getStatus() {
        return transactionStatus;
    }

    @Override
    public void readyToClose(DATA current) {
        synchronized (this) {
            Preconditions.checkState(transactionStatus.getState() == TransactionStatus.State.ResultReady);
            transactionStatus = transactionStatus.complete(current);
        }
    }

    private void transitionIfRunning(Supplier<TransactionStatus<DATA>> nextStatusSupplier) {
        synchronized (this) {
            if (transactionStatus.getState() == TransactionStatus.State.Started) {
                try {
                    transactionStatus = nextStatusSupplier.get();
                } catch (Exception e) {
                    transactionStatus = TransactionStatus.failed(e);
                }
            }
        }
    }
}
