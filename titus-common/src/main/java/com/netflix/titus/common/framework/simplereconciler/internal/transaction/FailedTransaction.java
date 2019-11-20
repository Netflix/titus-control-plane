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

import com.netflix.titus.common.framework.simplereconciler.internal.ChangeActionHolder;

public class FailedTransaction<DATA> implements Transaction<DATA> {

    private final ChangeActionHolder actionHolder;
    private final TransactionStatus<DATA> transactionStatus;

    public FailedTransaction(ChangeActionHolder actionHolder, Throwable error) {
        this.actionHolder = actionHolder;
        this.transactionStatus = TransactionStatus.failed(error);
    }

    @Override
    public void cancel() {
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
    public void readyToClose(DATA currentValue) {
    }
}
