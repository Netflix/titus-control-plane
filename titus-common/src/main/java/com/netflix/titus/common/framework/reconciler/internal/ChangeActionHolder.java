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

package com.netflix.titus.common.framework.reconciler.internal;

import com.netflix.titus.common.framework.reconciler.ChangeAction;

final class ChangeActionHolder {

    private final ChangeAction changeAction;
    private final long transactionId;
    private final long createTimeMs;

    ChangeActionHolder(ChangeAction changeAction, long transactionId, long createTimeMs) {
        this.changeAction = changeAction;
        this.transactionId = transactionId;
        this.createTimeMs = createTimeMs;
    }

    ChangeAction getChangeAction() {
        return changeAction;
    }

    long getTransactionId() {
        return transactionId;
    }

    long getCreateTimeMs() {
        return createTimeMs;
    }
}
