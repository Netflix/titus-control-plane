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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.annotation.Nullable;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

final class ChangeActionHolder<DATA> {

    private final Mono<Function<DATA, DATA>> action;
    private final String transactionId;
    private final long createTimestamp;
    private final MonoSink<DATA> subscriberSink;

    private AtomicBoolean cancelledRef = new AtomicBoolean();

    ChangeActionHolder(Mono<Function<DATA, DATA>> action,
                       String transactionId,
                       long createTimestamp,
                       @Nullable MonoSink<DATA> subscriberSink) {
        this.action = action;
        this.transactionId = transactionId;
        this.createTimestamp = createTimestamp;
        this.subscriberSink = subscriberSink;

        if (subscriberSink != null) {
            subscriberSink.onCancel(() -> cancelledRef.set(true));
        }
    }


    Mono<Function<DATA, DATA>> getAction() {
        return action;
    }

    String getTransactionId() {
        return transactionId;
    }

    long getCreateTimestamp() {
        return createTimestamp;
    }

    boolean isCancelled() {
        return cancelledRef.get();
    }

    @Nullable
    MonoSink<DATA> getSubscriberSink() {
        return subscriberSink;
    }
}
