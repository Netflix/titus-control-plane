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

package io.netflix.titus.master.endpoint.v2.rest.metric;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Timer;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.api.core.HttpResponseContext;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.dispatch.RequestDispatcher;
import io.netflix.titus.master.endpoint.v2.rest.RestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InstrumentedRequestDispatcher implements RequestDispatcher {
    private static final Logger logger = LoggerFactory.getLogger(InstrumentedRequestDispatcher.class);

    private final RequestDispatcher underlying;
    private final RestConfig config;
    private final Registry registry;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer latencyTimer;

    public InstrumentedRequestDispatcher(RequestDispatcher underlying, RestConfig config, Registry registry,
                                         Counter successCounter, Counter failureCounter, Timer latencyTimer) {
        this.underlying = underlying;
        this.config = config;
        this.registry = registry;
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
        this.latencyTimer = latencyTimer;
    }

    @Override
    public void dispatch(Object resource, HttpContext httpContext) {
        final long start = registry.clock().wallTime();
        try {
            underlying.dispatch(resource, httpContext);
            successCounter.increment();
        } catch (Exception e) {
            failureCounter.increment();
            if (config.isJaxrsErrorLoggingEnabled()) {
                logger.error(generateRequestResponseErrorMessage(httpContext, e));
            }
            throw e;
        } finally {
            final long end = registry.clock().wallTime();
            latencyTimer.record(end - start, TimeUnit.MILLISECONDS);
        }
    }

    private String generateRequestResponseErrorMessage(HttpContext context, Exception e) {
        StringBuilder result = new StringBuilder();
        HttpRequestContext request = context.getRequest();
        HttpResponseContext response = context.getResponse();
        result.append("An error occurred during an HTTP request:\r\n");
        if (request != null) {
            String bodyLengthString = request.getHeaderValue("Content-Length");
            result.append("Request Path: " + request.getMethod().toUpperCase() + " " + request.getRequestUri().toString() + "\r\n");
            result.append("Request Content-Length: " + bodyLengthString + "\r\n");
            result.append("Request Headers:\r\n" + request.getRequestHeaders()
                    .entrySet()
                    .stream()
                    .map(entry -> "\t" + entry.getKey() + ": " + entry.getValue() + "\r\n")
                    .collect(Collectors.joining())
            );

            long bodyLength = Strings.isNullOrEmpty(bodyLengthString) ? 0 : Long.parseLong(bodyLengthString);
            if (bodyLength > 0 && ((ContainerRequest) request).getEntityInputStream().markSupported()) {
                try {
                    ((ContainerRequest) request).getEntityInputStream().reset();
                    result.append("Request Body:\r\n" + request.getEntity(String.class) + "\r\n");
                } catch (Exception ignore) {
                }
            }
        }

        result.append("Error response http code: " + response.getStatus() + "\r\n");
        result.append("Error message: " + e.getMessage() + "\r\n");
        result.append("Error stack trace :\r\n" + Throwables.getStackTraceAsString(e) + "\r\n");

        return result.toString();
    }
}