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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.util.tuple.Either;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc.JobManagementServiceStub;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import org.junit.Test;
import rx.Observable;

import static com.netflix.titus.common.data.generator.DataGenerator.items;
import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static com.netflix.titus.common.data.generator.DataGenerator.zip;
import static io.grpc.Status.INTERNAL;
import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.NOT_FOUND;
import static io.grpc.Status.UNAVAILABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class ResponseAggregationTest {
    @Test
    public void singleValueReturnsFirstNonError() {
        List<CellResponse<JobManagementServiceStub, Either<String, Throwable>>> responses = generate(
                Either.ofValue("result1"),
                Either.ofError(UNAVAILABLE.asRuntimeException()),
                Either.ofError(NOT_FOUND.asRuntimeException())
        );

        CellResponse<JobManagementServiceStub, Either<String, Throwable>> merged = Observable.from(responses)
                .reduce(ResponseMerger.singleValue((e1, e2) -> e1))
                .toBlocking().single();

        assertThat(merged).isSameAs(responses.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void singleValueExpectsOnlyOneNonError() {
        Observable<CellResponse<JobManagementServiceStub, Either<String, Throwable>>> responses = Observable.from(generate(
                Either.ofValue("result1"),
                Either.ofValue("result2"),
                Either.ofError(NOT_FOUND.asRuntimeException()),
                Either.ofError(UNAVAILABLE.asRuntimeException()),
                Either.ofValue("result5")
        ));

        responses.reduce(ResponseMerger.singleValue((e1, e2) -> e1))
                .toBlocking().single();
    }

    @Test
    public void commonErrorMergerBasedOnCategories() {
        // one of each category
        Either<String, Throwable> notFound = Either.ofError(NOT_FOUND.asRuntimeException());
        Either<String, Throwable> transientError = Either.ofError(UNAVAILABLE.asRuntimeException());
        Either<String, Throwable> entityExistedInACell = Either.ofError(INVALID_ARGUMENT.asRuntimeException());
        Either<String, Throwable> unexpectedError = Either.ofError(INTERNAL.asRuntimeException());
        List<CellResponse<JobManagementServiceStub, Either<String, Throwable>>> responses = generate(
                notFound, transientError, entityExistedInACell, unexpectedError
        );
        CellResponse<JobManagementServiceStub, Either<String, Throwable>> unexpectedErrorResponse = responses.get(3);
        Collections.shuffle(responses);

        CellResponse<JobManagementServiceStub, Either<String, Throwable>> merged = Observable.from(responses)
                .reduce(ErrorMerger.grpc(FixedStatusOrder.common()))
                .toBlocking().single();

        // unexpected system errors have the highest precedence
        assertThat(merged).isSameAs(unexpectedErrorResponse);

        List<Status> sorted = responses.stream()
                .map(r -> Status.fromThrowable(r.getResult().getError()))
                .sorted(FixedStatusOrder.common())
                .collect(Collectors.toList());

        // verify common order: unexpected > entity existed > transient > not found
        assertThat(sorted).containsExactly(
                INTERNAL,
                INVALID_ARGUMENT,
                UNAVAILABLE,
                NOT_FOUND
        );
    }

    @SafeVarargs
    private static List<CellResponse<JobManagementServiceStub, Either<String, Throwable>>> generate(Either<String, Throwable>... values) {
        JobManagementServiceStub client = JobManagementServiceGrpc.newStub(mock(ManagedChannel.class));
        return zip(range(1), items(values)).map(pair -> {
            String idx = pair.getLeft().toString();
            Either<String, Throwable> result = pair.getRight();
            return new CellResponse<>(new Cell("cell" + idx, idx), client, result);
        }).toList();
    }

}
