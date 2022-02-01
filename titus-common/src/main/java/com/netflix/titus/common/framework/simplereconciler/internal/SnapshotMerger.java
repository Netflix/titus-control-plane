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

package com.netflix.titus.common.framework.simplereconciler.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.netflix.titus.common.util.tuple.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Helper class that given a list of {@link Flux} list emitters, merges first elements into a single list to build
 * one snapshot value.
 */
public class SnapshotMerger {

    public static <K, T> Flux<List<T>> mergeIndexedStreamWithSingleSnapshot(List<Flux<Pair<K, List<T>>>> indexedStreams) {
        return Flux.defer(() -> {
            ConcurrentMap<K, List<T>> snapshots = new ConcurrentHashMap<>();
            BlockingQueue<List<T>> updates = new LinkedBlockingQueue<>();

            return Flux.merge(indexedStreams).flatMap(pair -> {
                List<T> event = pair.getRight();
                if (snapshots.size() == indexedStreams.size()) {
                    return Mono.just(event);
                }

                K shardKey = pair.getLeft();
                if (snapshots.containsKey(shardKey)) {
                    updates.add(event);
                    return Flux.empty();
                }

                snapshots.put(shardKey, event);
                if (snapshots.size() < indexedStreams.size()) {
                    return Flux.empty();
                }

                // We can build full snapshot
                List<T> mergedSnapshot = new ArrayList<>();
                snapshots.forEach((idx, snapshot) -> mergedSnapshot.addAll(snapshot));

                List<List<T>> allEvents = new ArrayList<>();
                allEvents.add(mergedSnapshot);
                updates.drainTo(allEvents);
                return Flux.fromIterable(allEvents);
            });
        });
    }

    public static <T> Flux<List<T>> mergeWithSingleSnapshot(List<Flux<List<T>>> streams) {
        return Flux.defer(() -> {
            List<Flux<Pair<Integer, List<T>>>> indexedStreams = new ArrayList<>();
            for (int i = 0; i < streams.size(); i++) {
                int idx = i;
                indexedStreams.add(streams.get(i).map(e -> Pair.of(idx, e)));
            }

            return mergeIndexedStreamWithSingleSnapshot(indexedStreams);
        });

        /*
        return Flux.defer(() -> {
            List<Flux<Pair<Integer, List<T>>>> indexedStreams = new ArrayList<>();
            for (int i = 0; i < streams.size(); i++) {
                int idx = i;
                indexedStreams.add(streams.get(i).map(e -> Pair.of(idx, e)));
            }

            ConcurrentMap<Integer, List<T>> snapshots = new ConcurrentHashMap<>();
            BlockingQueue<List<T>> updates = new LinkedBlockingQueue<>();

            return Flux.merge(indexedStreams).flatMap(pair -> {
                List<T> event = pair.getRight();
                if (snapshots.size() == streams.size()) {
                    return Mono.just(event);
                }

                int shardIdx = pair.getLeft();
                if (snapshots.containsKey(shardIdx)) {
                    updates.add(event);
                    return Flux.empty();
                }

                snapshots.put(shardIdx, event);
                if (snapshots.size() < streams.size()) {
                    return Flux.empty();
                }

                // We can build full snapshot
                List<T> mergedSnapshot = new ArrayList<>();
                snapshots.forEach((idx, snapshot) -> mergedSnapshot.addAll(snapshot));

                List<List<T>> allEvents = new ArrayList<>();
                allEvents.add(mergedSnapshot);
                updates.drainTo(allEvents);
                return Flux.fromIterable(allEvents);
            });
        });
         */
    }
}
