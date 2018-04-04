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
}
