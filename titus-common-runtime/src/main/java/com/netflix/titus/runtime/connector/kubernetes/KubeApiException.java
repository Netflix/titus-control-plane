/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes;

import io.kubernetes.client.openapi.ApiException;

public class KubeApiException extends RuntimeException {

    private static final String NOT_FOUND = "Not Found";

    public enum ErrorCode {
        CONFLICT_ALREADY_EXISTS,
        INTERNAL,
        NOT_FOUND,
    }

    private final ErrorCode errorCode;

    public KubeApiException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = cause instanceof ApiException ? toErrorCode((ApiException) cause) : ErrorCode.INTERNAL;
    }

    public KubeApiException(ApiException cause) {
        this(String.format("%s: httpStatus=%s, body=%s", cause.getMessage(), cause.getCode(), cause.getResponseBody()), cause);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    private ErrorCode toErrorCode(ApiException e) {
        if (e.getMessage() == null) {
            return ErrorCode.INTERNAL;
        }
        if (e.getMessage().equalsIgnoreCase(NOT_FOUND)) {
            return ErrorCode.NOT_FOUND;
        }
        if (e.getCode() == 409 && e.getMessage().equals("Conflict") && e.getResponseBody().contains("AlreadyExists")) {
            return ErrorCode.CONFLICT_ALREADY_EXISTS;
        }
        return ErrorCode.INTERNAL;
    }
}
