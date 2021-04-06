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

package com.netflix.titus.api.model;

import com.google.common.base.Preconditions;

public class IdAndTimestampKey<T> implements PaginableEntityKey<T> {

    private final T entity;
    private final String id;
    private final long timestamp;

    public IdAndTimestampKey(T entity, String id, long timestamp) {
        Preconditions.checkNotNull(entity, "entity null");
        Preconditions.checkNotNull(id, "entity id null");

        this.entity = entity;
        this.id = id;
        this.timestamp = timestamp;
    }

    @Override
    public T getEntity() {
        return entity;
    }

    public String getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(PaginableEntityKey<T> other) {
        Preconditions.checkArgument(other instanceof IdAndTimestampKey);

        IdAndTimestampKey<T> otherKey = (IdAndTimestampKey<T>) other;
        int cmp = Long.compare(timestamp, otherKey.timestamp);
        if (cmp != 0) {
            return cmp;
        }
        return id.compareTo(otherKey.id);
    }
}
