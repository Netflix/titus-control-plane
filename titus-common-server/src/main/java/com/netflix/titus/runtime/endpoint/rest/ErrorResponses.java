/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.PropertyBindingException;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.NetworkExt;
import com.netflix.titus.common.util.StringExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.request.WebRequest;

/**
 * Collection of helper functions to build {@link ErrorResponse} instances.
 */
public final class ErrorResponses {

    private static final Logger logger = LoggerFactory.getLogger(ErrorResponses.class);


    private static final ObjectMapper errorMapper = new ObjectMapper();

    private ErrorResponses() {
    }

    public static ErrorResponse noRequestBody(HttpServletRequest httpServletRequest) {
        return ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, "empty request body")
                .clientRequest(httpServletRequest)
                .serverContext()
                .threadContext()
                .build();
    }

    public static ErrorResponse badRequest(HttpServletRequest httpServletRequest, String message) {
        return ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, message)
                .clientRequest(httpServletRequest)
                .serverContext()
                .threadContext()
                .build();
    }

    public static ErrorResponse badRequest(HttpServletRequest httpServletRequest, String message, Exception cause) {
        return ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, message)
                .clientRequest(httpServletRequest)
                .serverContext()
                .threadContext()
                .exceptionContext(cause)
                .build();
    }

    public static ErrorResponse badRequest(HttpServletRequest httpServletRequest, Exception cause) {
        return badRequest(httpServletRequest, toMessageChain(cause), cause);
    }

    public static ErrorResponse internalServerError(HttpServletRequest httpServletRequest, Exception cause) {
        return ErrorResponse.newError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, toMessageChain(cause))
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(cause)
                .build();
    }

    public static void sendError(HttpServletResponse httpServletResponse, ErrorResponse.ErrorResponseBuilder errorResponseBuilder) {
        sendError(httpServletResponse, errorResponseBuilder.build());
    }

    public static void sendError(HttpServletResponse httpServletResponse, ErrorResponse errorResponse) {
        httpServletResponse.setStatus(errorResponse.getStatusCode());
        httpServletResponse.setContentType("application/json");

        try (PrintWriter writer = httpServletResponse.getWriter()) {
            errorMapper.writeValue(writer, errorResponse);
        } catch (IOException e) {
            // Do not propagate this error further, just leave log entry
            logger.warn("Reply body serialization error", e);
        }
    }

    private static String toMessageChain(Throwable cause) {
        if (cause.getCause() == null) {
            return getDesiredMessage(cause);
        }
        StringBuilder sb = new StringBuilder();
        for (Throwable ex = cause; ex != null; ex = ex.getCause()) {
            sb.append(getDesiredMessage(cause)).append('(').append(ex.getClass().getSimpleName()).append(')');
            if (ex.getCause() != null) {
                sb.append(" -->  ");
            }
        }
        return sb.toString();
    }

    private static String getDesiredMessage(Throwable cause) {
        if (cause instanceof JsonProcessingException) {
            return ((JsonProcessingException) cause).getOriginalMessage();
        }
        return cause.getMessage();
    }

    static Map<String, Object> buildHttpRequestContext(HttpServletRequest req) {
        Map<String, Object> result = new TreeMap<>();

        StringBuilder uriBuilder = new StringBuilder(req.getServletPath());
        if (req.getPathInfo() != null) {
            uriBuilder.append(req.getPathInfo());
        }
        if (req.getQueryString() != null) {
            uriBuilder.append('?').append(req.getQueryString());
        }
        result.put("relativeURI", uriBuilder.toString());
        result.put("method", req.getMethod());

        Enumeration<String> headerIt = req.getHeaderNames();
        while (headerIt.hasMoreElements()) {
            String header = headerIt.nextElement();
            result.put(header, req.getHeader(header));
        }

        return result;
    }

    public static Map<String, Object> buildWebRequestContext(WebRequest webRequest) {
        Map<String, Object> result = new TreeMap<>();

        result.put("relativeURI", StringExt.safeTrim(webRequest.getContextPath()));
        result.put("secure", webRequest.isSecure());
        Evaluators.acceptNotNull(webRequest.getRemoteUser(), user -> result.put("remoteUser", user));

        return result;
    }

    /**
     * Collect basic information about the server.
     */
    static Map<String, Object> buildServerContext() {
        Map<String, Object> result = new TreeMap<>();

        Optional<String> hostName = NetworkExt.getHostName();
        if (hostName.isPresent()) {
            result.put("hostName", hostName.get());
        }
        Optional<List<String>> localIPs = NetworkExt.getLocalIPs();
        if (localIPs.isPresent()) {
            List<String> nonLoopbackIPs = localIPs.get().stream().filter(ip -> !NetworkExt.isLoopbackIP(ip)).collect(Collectors.toList());
            result.put("ipV4", nonLoopbackIPs.stream().filter(ip -> !NetworkExt.isIPv6(ip)).collect(Collectors.toList()));
            result.put("ipV6", nonLoopbackIPs.stream().filter(NetworkExt::isIPv6).collect(Collectors.toList()));
        }
        return result;
    }

    static List<StackTraceRepresentation> buildExceptionContext(Throwable cause) {
        List<StackTraceRepresentation> stackTraces = new ArrayList<>();

        for (Throwable currentCause = cause; currentCause != null; currentCause = currentCause.getCause()) {
            stackTraces.add(new StackTraceRepresentation(currentCause));
        }
        return stackTraces;
    }

    static List<String> buildThreadContext() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        int offset = 0;
        while (offset < stackTrace.length && !isErrorFrameworkClass(stackTrace[offset].getClassName())) {
            offset++;
        }
        while (offset < stackTrace.length && isErrorFrameworkClass(stackTrace[offset].getClassName())) {
            offset++;
        }

        ArrayList<String> result = new ArrayList<>();
        for (; offset < stackTrace.length; offset++) {
            result.add(stackTrace[offset].toString());
        }
        return result;
    }

    private static boolean isErrorFrameworkClass(String className) {
        return ErrorResponse.class.getName().equals(className)
                || ErrorResponses.class.getName().equals(className)
                || ErrorResponse.ErrorResponseBuilder.class.getName().equals(className);
    }

    static class StackTraceRepresentation {

        private final String message;
        private final String type;
        private final String typeLocation;
        private final Map<String, Object> details;
        private final List<String> stackTrace;

        private StackTraceRepresentation(Throwable cause) {
            this.type = cause.getClass().getName();
            this.typeLocation = getSourceOf(cause.getClass());
            this.message = getDesiredMessage(cause);
            this.stackTrace = getStackTraceOf(cause);

            if (cause instanceof JsonProcessingException) {
                this.details = appendJacksonErrorDetails((JsonProcessingException) cause);
            } else {
                this.details = null;
            }
        }

        public String getType() {
            return type;
        }

        public String getTypeLocation() {
            return typeLocation;
        }

        public String getMessage() {
            return message;
        }

        public Map<String, Object> getDetails() {
            return details;
        }

        public List<String> getStackTrace() {
            return stackTrace;
        }

        private String getSourceOf(Class<? extends Throwable> aClass) {
            URL location = aClass.getResource('/' + aClass.getName().replace('.', '/') + ".class");
            if (location == null) {
                return null;
            }
            return location.toString();
        }

        private ArrayList<String> getStackTraceOf(Throwable cause) {
            ArrayList<String> stackTrace = new ArrayList<>(cause.getStackTrace().length);
            for (StackTraceElement element : cause.getStackTrace()) {
                stackTrace.add(element.toString());
            }
            return stackTrace;
        }

        private Map<String, Object> appendJacksonErrorDetails(JsonProcessingException cause) {
            Map<String, Object> out = new TreeMap<>();

            JsonLocation location = cause.getLocation();
            if (location != null) {
                out.put("errorLocation", "line: " + location.getLineNr() + ", column: " + location.getColumnNr());
                if (location.getSourceRef() != null && location.getSourceRef() instanceof String) {
                    out.put("document", location.getSourceRef());
                }
            }

            if (cause instanceof JsonMappingException) {
                JsonMappingException mappingEx = (JsonMappingException) cause;
                if (mappingEx.getPathReference() != null) {
                    out.put("pathReference", mappingEx.getPathReference());
                }

                if (cause instanceof InvalidFormatException) {
                    InvalidFormatException formEx = (InvalidFormatException) cause;
                    if (formEx.getTargetType() != null) {
                        out.put("targetType", formEx.getTargetType().getName());
                    }
                } else if (cause instanceof PropertyBindingException) {
                    PropertyBindingException bindingEx = (PropertyBindingException) cause;
                    if (bindingEx.getPropertyName() != null) {
                        out.put("property", bindingEx.getPropertyName());
                        out.put("knownProperties", bindingEx.getKnownPropertyIds());
                    }
                }
            }

            return out;
        }
    }
}
