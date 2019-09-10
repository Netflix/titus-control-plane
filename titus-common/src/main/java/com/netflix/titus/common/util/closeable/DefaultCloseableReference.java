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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.netflix.titus.common.util.ExceptionExt;

class DefaultCloseableReference<T> implements CloseableReference<T> {

    private final T resource;
    private final Runnable closeAction;
    private final boolean serialize;
    private final boolean swallowException;
    private final Runnable onSuccessCallback;
    private final Consumer<Throwable> onErrorCallback;

    private final AtomicBoolean closed = new AtomicBoolean();

    DefaultCloseableReference(T resource,
                              Runnable closeAction,
                              boolean serialize,
                              boolean swallowException,
                              Runnable onSuccessCallback,
                              Consumer<Throwable> onErrorCallback) {
        this.resource = resource;
        this.closeAction = closeAction;
        this.serialize = serialize;
        this.swallowException = swallowException;
        this.onSuccessCallback = onSuccessCallback;
        this.onErrorCallback = onErrorCallback;
    }

    @Override
    public T get() {
        return resource;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (isClosed()) {
            return;
        }
        try {
            doClose();
            ExceptionExt.silent(onSuccessCallback::run);
        } catch (Throwable e) {
            ExceptionExt.silent(() -> onErrorCallback.accept(e));
            if (!swallowException) {
                throw e;
            }
        }
    }

    private void doClose() {
        if (serialize) {
            synchronized (closed) {
                closeAction.run();
            }
        } else {
            closeAction.run();
        }
    }
}
