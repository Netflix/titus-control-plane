/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded.kube.event;

import java.util.function.Function;

public class EmbeddedKubeEvent<T> {

    public enum Kind {
        ADDED,
        UPDATED,
        DELETED
    }

    private final Kind kind;
    private final T current;
    private final T previous;

    private EmbeddedKubeEvent(Kind kind, T current, T previous) {
        this.kind = kind;
        this.current = current;
        this.previous = previous;
    }

    public Kind getKind() {
        return kind;
    }

    public T getCurrent() {
        return current;
    }

    public T getPrevious() {
        return previous;
    }

    public <V> EmbeddedKubeEvent<V> map(Function<T, V> mapper) {
        V currentMapped = mapper.apply(current);
        V previousMapped = previous == null ? null : mapper.apply(previous);
        return new EmbeddedKubeEvent<>(kind, currentMapped, previousMapped);
    }

    public static <T> EmbeddedKubeEvent<T> added(T current) {
        return new EmbeddedKubeEvent<>(Kind.ADDED, current, null);
    }

    public static <T> EmbeddedKubeEvent<T> updated(T current, T previous) {
        return new EmbeddedKubeEvent<>(Kind.UPDATED, current, previous);
    }

    public static <T> EmbeddedKubeEvent<T> deleted(T current) {
        return new EmbeddedKubeEvent<>(Kind.DELETED, current, null);
    }
}
