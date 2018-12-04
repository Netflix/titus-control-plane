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

package com.netflix.titus.common.util.time;

public interface Clock {
    /**
     * Time elapsed in nanoseconds.
     */
    long nanoTime();

    /**
     * Current time in milliseconds, equivalent to {@link System#currentTimeMillis()}.
     */
    long wallTime();

    /**
     * Returns true, of the current time is past the given timestamp.
     */
    default boolean isPast(long timestamp) {
        return wallTime() > timestamp;
    }
}
