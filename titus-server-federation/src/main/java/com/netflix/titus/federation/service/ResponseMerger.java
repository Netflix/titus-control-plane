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

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.tuple.Either;
import io.grpc.stub.AbstractStub;
import rx.functions.Func2;

interface ResponseMerger<STUB extends AbstractStub<STUB>, T> extends Func2<
        CellResponse<STUB, T>,
        CellResponse<STUB, T>,
        CellResponse<STUB, T>> {
    // generics sanity

    /**
     * Merge results by returning the first non-error value encountered. Otherwise merge all error results with a
     * fallback merger.
     *
     * @param errorMerger fallback merger for errors
     */
    static <STUB extends AbstractStub<STUB>, T>
    ResponseMerger<STUB, Either<T, Throwable>> singleValue(ErrorMerger<STUB, T> errorMerger) {
        return (one, other) -> {
            if (isEmptyResponseMarker(one)) {
                return other;
            }
            if (isEmptyResponseMarker(other)) {
                return one;
            }
            boolean bothHaveValues = one.getResult().hasValue() && other.getResult().hasValue();
            Preconditions.checkArgument(!bothHaveValues, "expecting a single non-error response");
            if (one.getResult().hasValue()) {
                return one;
            }
            if (other.getResult().hasValue()) {
                return other;
            }
            return errorMerger.call(one, other);
        };
    }

    final CellResponse<?, ?> EMPTY_RESPONSE_MARKER = new CellResponse<>(null, null, null);

    /**
     * Marker to be used with {@link rx.Observable#reduce(Object, Func2)} and {@link rx.Observable#reduce(Func2)} on
     * empty Observables. By design, <tt>reduce</tt> emits an error for empty Observables. This marker can be used as
     * the initial seed, and will be passed through in case the <tt>Observable</tt> is empty, and can be filtered out
     * later with {@link ResponseMerger#isNotEmptyResponseMarker(CellResponse)}. Merger functions _must_ ignore the
     * empty marker with {@link ResponseMerger#isEmptyResponseMarker(CellResponse)}.
     * <p>
     * See https://github.com/ReactiveX/RxJava/pull/474
     */
    @SuppressWarnings("unchecked")
    static <STUB extends AbstractStub<STUB>, T> CellResponse<STUB, T> emptyResponseMarker() {
        return (CellResponse<STUB, T>) EMPTY_RESPONSE_MARKER;
    }

    static <STUB extends AbstractStub<STUB>, T> boolean isEmptyResponseMarker(CellResponse<STUB, T> response) {
        return response == EMPTY_RESPONSE_MARKER;
    }

    static <STUB extends AbstractStub<STUB>, T> boolean isNotEmptyResponseMarker(CellResponse<STUB, T> response) {
        return response != EMPTY_RESPONSE_MARKER;
    }

    static <STUB extends AbstractStub<STUB>, T> ResponseMerger<STUB, Either<T, Throwable>> singleValue() {
        return singleValue(ErrorMerger.<STUB, T>grpcWithDefaultPriorities());
    }
}
