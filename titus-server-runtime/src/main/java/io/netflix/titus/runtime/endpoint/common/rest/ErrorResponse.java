/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.runtime.endpoint.common.rest;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import io.netflix.titus.api.json.ObjectMappers;

/**
 * Error representation returned as JSON document for failed REST requests.
 */
public class ErrorResponse {

    public static final String CLIENT_REQUEST = "clientRequest";
    public static final String THREAD_CONTEXT = "threadContext";
    public static final String SERVER_CONTEXT = "serverContext";
    public static final String EXCEPTION_CONTEXT = "exception";

    @JsonView(ObjectMappers.PublicView.class)
    private final int statusCode;

    @JsonView(ObjectMappers.PublicView.class)
    private final String message;

    @JsonView(ObjectMappers.PublicView.class)
    private final Object errorDetails;

    @JsonView(ObjectMappers.DebugView.class)
    private final Map<String, Object> errorContext;

    @JsonCreator
    private ErrorResponse(@JsonProperty("statusCode") int statusCode,
                          @JsonProperty("message") String message,
                          @JsonProperty("errorDetails") Object errorDetails,
                          @JsonProperty("errorContext") Map<String, Object> errorContext) {
        this.statusCode = statusCode;
        this.message = message;
        this.errorDetails = errorDetails;
        this.errorContext = errorContext;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getMessage() {
        return message;
    }

    public Object getErrorDetails() {
        return errorDetails;
    }

    /**
     * Arbitrary additional information that can be attached to an error. The only requirement is that
     * it must be serializable by Jackson.
     */
    public Map<String, Object> getErrorContext() {
        return errorContext;
    }

    public static ErrorResponseBuilder newError(int statusCode) {
        return new ErrorResponseBuilder().status(statusCode);
    }

    public static ErrorResponseBuilder newError(int statusCode, String message) {
        return new ErrorResponseBuilder().status(statusCode).message(message);
    }

    public static class ErrorResponseBuilder {
        private int statusCode;
        private String message;
        private Map<String, Object> errorContext = new TreeMap<>();
        private Object errorDetails;

        public ErrorResponseBuilder status(int statusCode) {
            this.statusCode = statusCode;
            return this;
        }

        public ErrorResponseBuilder message(String message) {
            this.message = message;
            return this;
        }

        public ErrorResponseBuilder errorDetails(Object errorDetails) {
            this.errorDetails = errorDetails;
            return this;
        }

        public ErrorResponseBuilder clientRequest(HttpServletRequest httpRequest) {
            return withContext(CLIENT_REQUEST, ErrorResponses.buildHttpRequestContext(httpRequest));
        }

        public ErrorResponseBuilder threadContext() {
            return withContext(THREAD_CONTEXT, ErrorResponses.buildThreadContext());
        }

        public ErrorResponseBuilder serverContext() {
            return withContext(SERVER_CONTEXT, ErrorResponses.buildServerContext());
        }

        public ErrorResponseBuilder exceptionContext(Throwable cause) {
            return withContext(EXCEPTION_CONTEXT, ErrorResponses.buildExceptionContext(cause));
        }

        public ErrorResponseBuilder withContext(String name, Object details) {
            if (details == null) {
                errorContext.remove(name);
            } else {
                errorContext.put(name, details);
            }
            return this;
        }

        public ErrorResponse build() {
            return new ErrorResponse(
                    statusCode,
                    message,
                    errorDetails,
                    errorContext.isEmpty() ? null : Collections.unmodifiableMap(errorContext)
            );
        }
    }
}
