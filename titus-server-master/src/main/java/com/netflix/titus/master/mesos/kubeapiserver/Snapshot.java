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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

/**
 * Utility class that makes creating a snapshot of data at a certain point in time easier.
 */
public class Snapshot<T> {

    private static final Snapshot EMPTY_SNAPSHOT = new Snapshot<>(Collections.emptyList(), 0);

    private final List<T> items;
    private final long timestamp;

    private Snapshot(Collection<T> collection, long timestamp) {
        items = new ArrayList<>(collection);
        this.timestamp = timestamp;
    }

    /**
     * Create new snapshot
     */
    public static <T> Snapshot<T> newSnapshot(Collection<T> collection, long timestamp) {
        return new Snapshot<>(collection, timestamp);
    }

    /**
     * Get empty snapshot. Note that the empty snapshot has of timestamp of 0.
     */
    public static <T> Snapshot<T> emptySnapshot() {
        return EMPTY_SNAPSHOT;
    }

    /**
     * Get the items in this snapshot.
     */
    public List<T> getItems() {
        return Collections.unmodifiableList(items);
    }

    /**
     * Get the timestamp of this snapshot.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Compare this snapshot with another snapshot. Note that the passed in snapshot is considered to be the first snapshot
     * when doing the comparison.
     */
    public SnapshotComparison<T> compare(Snapshot<T> snapshot, Function<T, String> identityFn, BiPredicate<T, T> equalFn) {
        return SnapshotComparison.compare(snapshot, this, identityFn, equalFn);
    }
}
