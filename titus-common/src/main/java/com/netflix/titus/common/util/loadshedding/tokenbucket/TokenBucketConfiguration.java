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

package com.netflix.titus.common.util.loadshedding.tokenbucket;

import java.util.Objects;
import java.util.regex.Pattern;

public class TokenBucketConfiguration {

    /**
     * A unique token bucket identifier.
     */
    private final String name;

    /**
     * Execution priority order (lower value == higher priority).
     */
    private final int order;

    /**
     * If true, token bucket is shared by all matching callers. If false, a new bucket is created for each caller name.
     */
    private final boolean shared;

    /**
     * Caller pattern.
     */
    private final String callerPatternString;

    /**
     * Called endpoint pattern.
     */
    private final String endpointPatternString;

    /**
     * Total capacity of the bucket. It is also its initial state (bucket starts full).
     */
    private final int capacity;

    /**
     * How many tokens should be added each second.
     */
    private final int refillRateInSec;

    private final Pattern callerPattern;
    private final Pattern endpointPattern;

    public TokenBucketConfiguration(String name,
                                    int order,
                                    boolean shared,
                                    String callerPatternString,
                                    String endpointPatternString,
                                    int capacity,
                                    int refillRateInSec) {
        this.name = name;
        this.order = order;
        this.shared = shared;
        this.callerPatternString = callerPatternString;
        this.endpointPatternString = endpointPatternString;
        this.capacity = capacity;
        this.refillRateInSec = refillRateInSec;

        this.callerPattern = Pattern.compile(callerPatternString);
        this.endpointPattern = Pattern.compile(endpointPatternString);
    }

    public String getName() {
        return name;
    }

    public int getOrder() {
        return order;
    }

    public boolean isShared() {
        return shared;
    }

    public String getCallerPatternString() {
        return callerPatternString;
    }

    public String getEndpointPatternString() {
        return endpointPatternString;
    }

    public Pattern getCallerPattern() {
        return callerPattern;
    }

    public Pattern getEndpointPattern() {
        return endpointPattern;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getRefillRateInSec() {
        return refillRateInSec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TokenBucketConfiguration that = (TokenBucketConfiguration) o;
        return order == that.order &&
                shared == that.shared &&
                capacity == that.capacity &&
                refillRateInSec == that.refillRateInSec &&
                Objects.equals(name, that.name) &&
                Objects.equals(callerPatternString, that.callerPatternString) &&
                Objects.equals(endpointPatternString, that.endpointPatternString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, order, shared, callerPatternString, endpointPatternString, capacity, refillRateInSec, callerPattern, endpointPattern);
    }

    @Override
    public String toString() {
        return "TokenBucketConfiguration{" +
                "name='" + name + '\'' +
                ", order=" + order +
                ", shared=" + shared +
                ", callerPatternString='" + callerPatternString + '\'' +
                ", endpointPatternString='" + endpointPatternString + '\'' +
                ", capacity=" + capacity +
                ", refillRateInSec=" + refillRateInSec +
                ", callerPattern=" + callerPattern +
                ", endpointPattern=" + endpointPattern +
                '}';
    }
}
