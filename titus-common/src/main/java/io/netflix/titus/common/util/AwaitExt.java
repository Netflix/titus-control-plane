/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.util;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A collection of functions for polling/executing a predicate periodically until it succeeds or timeout occurs.
 */
public final class AwaitExt {

    private AwaitExt() {
    }

    /**
     * Periodically executes a provided predicate, and returns immediately when succeeds (return true). Returns
     * false if timeout occurs.
     */
    public static boolean awaitUntil(Supplier<Boolean> predicate, long timeout, TimeUnit timeUnit) throws InterruptedException {
        long endTime = System.currentTimeMillis() + timeUnit.toMillis(timeout);
        long delayMs = 1;
        do {
            long now = System.currentTimeMillis();
            boolean currentStatus = predicate.get();
            if (currentStatus || now >= endTime) {
                return currentStatus;
            }
            Thread.sleep(delayMs);
            delayMs *= 2;
        } while (true);
    }
}
