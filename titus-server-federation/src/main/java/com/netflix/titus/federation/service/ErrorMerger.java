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

import java.util.Comparator;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.tuple.Either;
import io.grpc.Status;
import io.grpc.stub.AbstractStub;
import rx.functions.Func2;

import static com.netflix.titus.common.grpc.GrpcUtil.isNotOK;

interface ErrorMerger<STUB extends AbstractStub<STUB>, T> extends Func2<
        CellResponse<STUB, Either<T, Throwable>>,
        CellResponse<STUB, Either<T, Throwable>>,
        CellResponse<STUB, Either<T, Throwable>>> {
    // generics sanity

    /**
     * Rank errors with a provided {@link Comparator} by gRPC error codes, and return the most important one. In case of
     * a tie, the return is undefined and can be any of the most important errors.
     */
    static <STUB extends AbstractStub<STUB>, T> ErrorMerger<STUB, T> grpc(Comparator<Status> rank) {
        return (one, other) -> {
            Status oneStatus = Status.fromThrowable(one.getResult().getError());
            Status otherStatus = Status.fromThrowable(other.getResult().getError());
            Preconditions.checkArgument(isNotOK(oneStatus), "status is not an error");
            Preconditions.checkArgument(isNotOK(otherStatus), "status is not an error");

            if (rank.compare(oneStatus, otherStatus) <= 0) {
                return one;
            }
            return other;
        };
    }

    static <STUB extends AbstractStub<STUB>, T> ErrorMerger<STUB, T> grpcWithDefaultPriorities() {
        return grpc(StatusCategoryComparator.defaultPriorities());
    }
}
