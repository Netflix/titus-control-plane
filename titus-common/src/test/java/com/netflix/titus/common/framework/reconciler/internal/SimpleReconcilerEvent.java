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

import java.util.Optional;

/**
 *
 */
public class SimpleReconcilerEvent {

    enum EventType {
        Checkpoint,
        ChangeRequest,
        Changed,
        ChangeError,
        ModelInitial,
        ModelUpdated,
        ModelUpdateError
    }

    private final long timestamp;

    private final EventType eventType;
    private final String message;
    private final Optional<Throwable> error;

    public SimpleReconcilerEvent(EventType eventType, String message, Optional<Throwable> error) {
        this.eventType = eventType;
        this.message = message;
        this.error = error;
        this.timestamp = System.currentTimeMillis();
    }

    public EventType getEventType() {
        return eventType;
    }

    public String getMessage() {
        return message;
    }

    public Optional<Throwable> getError() {
        return error;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "SimpleReconcilerEvent{" +
                "timestamp=" + timestamp +
                ", eventType=" + eventType +
                ", message='" + message + '\'' +
                error.map(e -> ", error=" + e.getMessage() + '\'').orElse("") +
                '}';
    }

    public static SimpleReconcilerEvent newChange(String changedValue) {
        return new SimpleReconcilerEvent(EventType.Changed, changedValue, Optional.empty());
    }
}
