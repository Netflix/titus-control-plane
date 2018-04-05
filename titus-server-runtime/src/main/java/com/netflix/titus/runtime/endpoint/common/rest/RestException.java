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

/**
 *
 */
public class RestException extends RuntimeException {

    private final int statusCode;
    private final String message;
    private final Object details;
    private final Throwable cause;

    private RestException(int statusCode,
                          String message,
                          Object details,
                          Throwable cause) {
        super();
        this.statusCode = statusCode;
        this.message = message;
        this.details = details;
        this.cause = cause;
    }

    public int getStatusCode() {
        return statusCode;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public Object getDetails() {
        return details;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    public static RestExceptionBuilder newBuilder(int statusCode, String message) {
        return new RestExceptionBuilder(statusCode, message);
    }

    public static final class RestExceptionBuilder {
        private int statusCode;
        private String message;
        private Object details;
        private Throwable cause;

        private RestExceptionBuilder(int statusCode, String message) {
            this.statusCode = statusCode;
            this.message = message;
        }

        public RestExceptionBuilder withDetails(Object details) {
            this.details = details;
            return this;
        }

        public RestExceptionBuilder withCause(Throwable cause) {
            this.cause = cause;
            return this;
        }

        public RestExceptionBuilder but() {
            return new RestExceptionBuilder(statusCode, message).withDetails(details).withCause(cause);
        }

        public RestException build() {
            return new RestException(statusCode, message, details, cause);
        }
    }
}
