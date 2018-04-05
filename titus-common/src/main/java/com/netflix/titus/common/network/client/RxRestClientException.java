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

package com.netflix.titus.common.network.client;

import java.util.Optional;

public class RxRestClientException extends RuntimeException {

    private final int statusCode;
    private final Optional<Object> errorBody;

    public RxRestClientException(String message) {
        this(-1, message);
    }

    public RxRestClientException(int statusCode, String message) {
        this(statusCode, message, Optional.empty());
    }

    public RxRestClientException(int statusCode, String message, Optional<Object> errorBody) {
        super(message + " (HTTP status code=" + statusCode + ')');
        this.statusCode = statusCode;
        this.errorBody = errorBody;
    }

    public RxRestClientException(int statusCode) {
        this(statusCode, statusCode + " returned in HTTP response");
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Optional<Object> getErrorBody() {
        return errorBody;
    }
}
