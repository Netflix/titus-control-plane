/*
 * Copyright 2020 Netflix, Inc.
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

import javax.annotation.PreDestroy;

/**
 * Dependency injected instrumented wrapper for {@link CloseableReference}.
 */
public class CloseableDependency<T> {

    private final CloseableReference<T> closeableReference;

    private CloseableDependency(CloseableReference<T> closeableReference) {
        this.closeableReference = closeableReference;
    }

    public T get() {
        return closeableReference.get();
    }

    @PreDestroy
    public void close() {
        closeableReference.close();
    }

    public static <T> CloseableDependency<T> of(CloseableReference<T> closeableReference) {
        return new CloseableDependency<>(closeableReference);
    }
}
