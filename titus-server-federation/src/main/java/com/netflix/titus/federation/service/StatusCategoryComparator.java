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

package com.netflix.titus.federation.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.grpc.Status;

import static io.grpc.Status.Code.ABORTED;
import static io.grpc.Status.Code.ALREADY_EXISTS;
import static io.grpc.Status.Code.CANCELLED;
import static io.grpc.Status.Code.DATA_LOSS;
import static io.grpc.Status.Code.DEADLINE_EXCEEDED;
import static io.grpc.Status.Code.FAILED_PRECONDITION;
import static io.grpc.Status.Code.INTERNAL;
import static io.grpc.Status.Code.INVALID_ARGUMENT;
import static io.grpc.Status.Code.NOT_FOUND;
import static io.grpc.Status.Code.OUT_OF_RANGE;
import static io.grpc.Status.Code.PERMISSION_DENIED;
import static io.grpc.Status.Code.RESOURCE_EXHAUSTED;
import static io.grpc.Status.Code.UNAUTHENTICATED;
import static io.grpc.Status.Code.UNAVAILABLE;
import static io.grpc.Status.Code.UNIMPLEMENTED;
import static io.grpc.Status.Code.UNKNOWN;

/**
 * Group gRPC {@link Status} codes in categories (buckets) with different precedence. More important errors take place
 * of less important errors.
 * <p>
 * Errors not explicitly provided are considered less important than the ones explicitly provided.
 */
class StatusCategoryComparator implements Comparator<Status> {
    private final Map<Status.Code, Integer> priorities = new HashMap<>();

    @SafeVarargs
    StatusCategoryComparator(List<Status.Code>... statusCategories) {
        for (int i = 0; i < statusCategories.length; i++) {
            List<Status.Code> codes = statusCategories[i];
            for (Status.Code code : codes) {
                if (priorities.putIfAbsent(code, i) != null) {
                    throw new IllegalArgumentException(code + " has been already specified with a different priority");
                }
            }
        }
    }

    @Override
    public int compare(Status one, Status other) {
        if (!priorities.containsKey(one.getCode())) {
            return 1;
        }
        if (!priorities.containsKey(other.getCode())) {
            return -1;
        }
        return priorities.get(one.getCode()).compareTo(priorities.get(other.getCode()));
    }

    private static final StatusCategoryComparator DEFAULT = new StatusCategoryComparator(
            // unexpected system errors first
            Arrays.asList(UNKNOWN, PERMISSION_DENIED, UNIMPLEMENTED, INTERNAL, DATA_LOSS, UNAUTHENTICATED),
            // then errors where the entity existed somewhere
            Arrays.asList(FAILED_PRECONDITION, INVALID_ARGUMENT, ALREADY_EXISTS, OUT_OF_RANGE),
            // then transient errors
            Arrays.asList(UNAVAILABLE, CANCELLED, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED),
            // NOT_FOUND for sure only if it came from everywhere
            Collections.singletonList(NOT_FOUND)
    );

    /**
     * A default categorization with priorities that should be good enough for most point query cases. The precedence
     * order is:
     * <p>
     * 1. Unexpected system errors.
     * 2. Errors where we can deduce the entity existed in a Cell (e.g.: validation errors).
     * 3. Transient errors.
     * 4. <tt>NOT_FOUND</tt> (we can only be sure if all errors are a <tt>NOT_FOUND</tt>)
     */
    static StatusCategoryComparator defaultPriorities() {
        return DEFAULT;
    }
}
