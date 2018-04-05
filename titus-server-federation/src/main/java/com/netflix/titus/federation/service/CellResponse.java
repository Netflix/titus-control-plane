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

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.common.util.tuple.Either;
import io.grpc.stub.AbstractStub;

class CellResponse<STUB extends AbstractStub<STUB>, T> {
    private final Cell cell;
    private final STUB client;
    private final T result;

    CellResponse(Cell cell, STUB client, T result) {
        this.cell = cell;
        this.client = client;
        this.result = result;
    }

    public Cell getCell() {
        return cell;
    }

    public STUB getClient() {
        return client;
    }

    public T getResult() {
        return result;
    }

    @Override
    public String toString() {
        return "CellResponse{" +
                "cell=" + cell +
                ", client=" + client +
                ", result=" + result +
                '}';
    }

    public static <STUB extends AbstractStub<STUB>, T>
    CellResponse<STUB, T> ofValue(CellResponse<STUB, Either<T, Throwable>> response) {
        return new CellResponse<>(response.getCell(), response.getClient(), response.getResult().getValue());
    }
}
