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

package com.netflix.titus.common.util.closeable;

import java.util.function.Consumer;

import com.google.common.base.Preconditions;

/**
 * A reference holder to a resource with attached close action.
 */
public interface CloseableReference<T> extends AutoCloseable {

    T get();

    boolean isClosed();

    /**
     * Override without exception in the signature.
     */
    @Override
    void close();

    static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    class Builder<T> {

        private static final Runnable NO_OP_ON_SUCCESS = () -> {
        };
        private static final Consumer<Throwable> NO_OP_ON_ERROR = e -> {
        };

        private T resource;
        private Runnable closeAction;
        private boolean serialize;
        private boolean swallowException;

        private Runnable onSuccessCallback = NO_OP_ON_SUCCESS;
        private Consumer<Throwable> onErrorCallback = NO_OP_ON_ERROR;

        public Builder<T> withResource(T resource) {
            this.resource = resource;
            return this;
        }

        public Builder<T> withCloseAction(Runnable closeAction) {
            this.closeAction = closeAction;
            return this;
        }

        public Builder<T> withSerialize(boolean serialize) {
            this.serialize = serialize;
            return this;
        }

        public Builder<T> withSwallowException(boolean swallowException) {
            this.swallowException = swallowException;
            return this;
        }

        public Builder<T> withOnSuccessCallback(Runnable onSuccessCallback) {
            this.onSuccessCallback = onSuccessCallback == null ? NO_OP_ON_SUCCESS : onSuccessCallback;
            return this;
        }

        public Builder<T> withOnErrorCallback(Consumer<Throwable> onErrorCallback) {
            this.onErrorCallback = onErrorCallback == null ? NO_OP_ON_ERROR : onErrorCallback;
            return this;
        }

        public CloseableReference<T> build() {
            Preconditions.checkNotNull(resource, "Closeable resource is null");
            Preconditions.checkNotNull(closeAction, "Closeable action is null");
            return new DefaultCloseableReference<>(resource, closeAction, serialize, swallowException, onSuccessCallback, onErrorCallback);
        }
    }
}
