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
import rx.Subscriber;

final class ChangeActionHolder {

    private final String entityHolderId;
    private final ChangeAction changeAction;
    private final Subscriber subscriber;
    private final String transactionId;
    private final long createTimestamp;

    ChangeActionHolder(String entityHolderId, ChangeAction changeAction, Subscriber subscriber, String transactionId, long createTimestamp) {
        this.entityHolderId = entityHolderId;
        this.changeAction = changeAction;
        this.subscriber = subscriber;
        this.transactionId = transactionId;
        this.createTimestamp = createTimestamp;
    }

    public String getEntityHolderId() {
        return entityHolderId;
    }

    ChangeAction getChangeAction() {
        return changeAction;
    }

    Subscriber<Void> getSubscriber() {
        return subscriber;
    }

    String getTransactionId() {
        return transactionId;
    }

    long getCreateTimestamp() {
        return createTimestamp;
    }
}
