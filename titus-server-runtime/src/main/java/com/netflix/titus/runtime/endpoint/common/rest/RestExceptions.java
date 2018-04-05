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

package com.netflix.titus.runtime.endpoint.common.rest;

import javax.servlet.http.HttpServletResponse;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.StatusRuntimeException;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public class RestExceptions {
    private static final int TOO_MANY_REQUESTS = 429;

    public static RestException from(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            return fromStatusRuntimeException((StatusRuntimeException) t);
        } else if (t instanceof InvalidProtocolBufferException) {
            return fromInvalidProtocolBufferException((InvalidProtocolBufferException) t);
        }

        return RestException.newBuilder(INTERNAL_SERVER_ERROR.getStatusCode(), t.getMessage())
                .withCause(t)
                .build();
    }

    private static RestException fromStatusRuntimeException(StatusRuntimeException e) {
        int statusCode;
        switch (e.getStatus().getCode()) {
            case OK:
                statusCode = HttpServletResponse.SC_OK;
                break;
            case INVALID_ARGUMENT:
                statusCode = HttpServletResponse.SC_BAD_REQUEST;
                break;
            case DEADLINE_EXCEEDED:
                statusCode = HttpServletResponse.SC_REQUEST_TIMEOUT;
                break;
            case NOT_FOUND:
                statusCode = HttpServletResponse.SC_NOT_FOUND;
                break;
            case ALREADY_EXISTS:
                statusCode = HttpServletResponse.SC_CONFLICT;
                break;
            case PERMISSION_DENIED:
                statusCode = HttpServletResponse.SC_FORBIDDEN;
                break;
            case RESOURCE_EXHAUSTED:
                statusCode = TOO_MANY_REQUESTS;
                break;
            case FAILED_PRECONDITION:
                statusCode = HttpServletResponse.SC_CONFLICT;
                break;
            case UNIMPLEMENTED:
                statusCode = HttpServletResponse.SC_NOT_IMPLEMENTED;
                break;
            case UNAVAILABLE:
                statusCode = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
                break;
            case UNAUTHENTICATED:
                statusCode = HttpServletResponse.SC_UNAUTHORIZED;
                break;
            default:
                statusCode = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        }
        return RestException.newBuilder(statusCode, e.getMessage())
                .withCause(e)
                .build();
    }

    private static RestException fromInvalidProtocolBufferException(InvalidProtocolBufferException e) {
        return RestException.newBuilder(BAD_REQUEST.getStatusCode(), e.getMessage())
                .withCause(e)
                .build();
    }
}
