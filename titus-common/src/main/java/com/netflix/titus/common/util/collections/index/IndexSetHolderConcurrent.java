/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.collections.index;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread safe implementation of {@link IndexSetHolder}, where updates may happen concurrently.
 * <p>
 * Based on: https://akarnokd.blogspot.com/2015/05/operator-concurrency-primitives_9.html
 */
public class IndexSetHolderConcurrent<PRIMARY_KEY, INPUT> implements IndexSetHolder<PRIMARY_KEY, INPUT> {

    private volatile IndexSet<PRIMARY_KEY, INPUT> indexSet;

    private final BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>();
    private final AtomicLong wip = new AtomicLong();

    public IndexSetHolderConcurrent(IndexSet<PRIMARY_KEY, INPUT> indexSet) {
        this.indexSet = indexSet;
    }

    @Override
    public IndexSet<PRIMARY_KEY, INPUT> getIndexSet() {
        return indexSet;
    }

    @Override
    public void add(Collection<INPUT> values) {
        queue.add(() -> indexSet = indexSet.add(values));
        drain();
    }

    @Override
    public void remove(Collection<PRIMARY_KEY> values) {
        queue.add(() -> indexSet = indexSet.remove(values));
        drain();
    }

    private void drain() {
        if (wip.getAndIncrement() == 0) {
            do {
                wip.lazySet(1);
                Runnable action;
                while ((action = queue.poll()) != null) {
                    action.run();
                }
            } while (wip.decrementAndGet() != 0);
        }
    }
}
