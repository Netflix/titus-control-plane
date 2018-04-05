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

package com.netflix.titus.common.util.limiter.tokenbucket;


import java.util.concurrent.TimeUnit;

/**
 * The strategy for refilling a {@link TokenBucket}.
 */
public interface RefillStrategy {

    /**
     * @return the number of tokens that should be added to the bucket
     */
    long refill();

    /**
     * @param unit the unit of the time
     * @return the time until the next refill occurs
     */
    long getTimeUntilNextRefill(TimeUnit unit);

}
