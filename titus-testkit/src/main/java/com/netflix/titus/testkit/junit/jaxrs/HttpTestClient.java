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

package com.netflix.titus.testkit.junit.jaxrs;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.network.client.RxHttpResponse;
import com.netflix.titus.common.network.client.RxRestClient;
import com.netflix.titus.common.network.client.RxRestClient.TypeProvider;
import com.netflix.titus.common.network.client.RxRestClientException;
import com.netflix.titus.common.network.client.RxRestClients;
import com.netflix.titus.common.network.client.TypeProviders;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.runtime.endpoint.common.rest.ErrorResponse;

import static java.util.Arrays.asList;

/**
 * HTTP client based on {@link HttpURLConnection} or REST API testing.
 */
public class HttpTestClient {

    private static final ObjectMapper MAPPER = createObjectMapper();

    private static final TypeProvider<ErrorResponse> ERROR_RESPONSE_TP = TypeProviders.of(ErrorResponse.class);

    private static final String CLIENT_ID = "testClient";

    private final String baseURI;
    private final RxRestClient rxClient;

    public HttpTestClient(String baseURI) {
        this.baseURI = baseURI;
        URI uri;
        try {
            uri = new URI(baseURI);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI format", e);
        }

        this.rxClient = RxRestClients.newBuilder(CLIENT_ID, new DefaultRegistry())
                .host(uri.getHost())
                .port(uri.getPort())
                .errorReplyTypeResolver(statusCode -> ERROR_RESPONSE_TP)
                .build();
    }

    public <T> T doGET(String path, Class<T> type) throws IOException {
        return handleErrors(() -> doGetInternal(path, Collections.emptyMap(), is -> MAPPER.readValue(is, type)));
    }

    public <T> T doGET(String path, Map<String, Object> queryParams, Class<T> type) throws IOException {
        return handleErrors(() -> doGetInternal(path, queryParams, is -> MAPPER.readValue(is, type)));
    }

    public <T> T doGET(String path, TypeReference<T> typeReference) throws IOException {
        MappingFunction<T> mappingFun = is -> MAPPER.readValue(is, typeReference);
        return handleErrors(() -> doGetInternal(path, Collections.emptyMap(), mappingFun));
    }

    public <T> T doGET(String path, Map<String, Object> queryParams, TypeReference<T> typeReference) throws IOException {
        MappingFunction<T> mappingFun = is -> MAPPER.readValue(is, typeReference);
        return handleErrors(() -> doGetInternal(path, queryParams, mappingFun));
    }

    public <T> T doGET(TypeReference<T> typeReference) throws IOException {
        return doGET("", typeReference);
    }

    public <T> void doPOST(T entity, String locationPath) throws IOException {
        handleErrors(() -> doPostInternal("", entity, locationPath));
    }

    public <I> void doPOST(String path, I entity, String locationPath) throws IOException {
        handleErrors(() -> doPostInternal(path, entity, locationPath));
    }

    public <I, O> O doPOST(String path, I entity, String locationPath, Class<O> replyType) throws IOException {
        return handleErrors(() -> doPostInternal(path, entity, locationPath, TypeProviders.of(replyType)));
    }

    public <T> void doPUT(String path, T entity) throws IOException {
        handleErrors(() -> doPutInternal(path, entity));
    }

    public void doDELETE(String path) throws IOException {
        handleErrors(() -> doDeleteInternal(path));
    }

    private <T> T doGetInternal(String path, Map<String, Object> queryParams, MappingFunction<T> mappingFun) throws IOException {
        UriBuilder uriBuilder = UriBuilder.fromPath(baseURI + fixPath(path));
        queryParams.forEach(uriBuilder::queryParam);
        URI uri = uriBuilder.build();

        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        try {
            connection.addRequestProperty("Accept", MediaType.APPLICATION_JSON);

            expectStatusCode(connection, Status.OK);
            expectReplyHeader(connection, "Content-Type", MediaType.APPLICATION_JSON);

            return mappingFun.readValue(connection.getInputStream());
        } finally {
            connection.disconnect();
        }
    }

    private <I> void doPostInternal(String path, I entity, String locationPath) throws IOException {
        TypeProvider<RxHttpResponse<Void>> wrappedType = TypeProviders.ofEmptyResponse();

        RxHttpResponse<Void> httpResponse = rxClient.doPOST(baseURI + fixPath(path), entity, wrappedType).toBlocking().first();
        if (!hasExpectedStatusCode(httpResponse.getStatusCode(), Status.CREATED, Status.NO_CONTENT)) {
            throw new HttpTestClientException(
                    httpResponse.getStatusCode(),
                    "Unexpected status code returned " + httpResponse.getStatusCode(),
                    httpResponse.getEntity()
            );
        }

        if (locationPath != null) {
            expectReplyHeader(httpResponse, "Location", baseURI + fixPath(locationPath));
        }
    }

    private <I, O> O doPostInternal(String path, I entity, String locationPath, TypeProvider<O> replyType) throws IOException {
        TypeProvider<RxHttpResponse<O>> wrappedType = TypeProviders.ofResponse(replyType);

        RxHttpResponse<O> httpResponse = rxClient.doPOST(baseURI + fixPath(path), entity, wrappedType).toBlocking().first();
        int statusCode = httpResponse.getStatusCode();

        if (!hasExpectedStatusCode(statusCode, Status.OK)) {
            throw new HttpTestClientException(
                    statusCode,
                    "Unexpected status code returned " + statusCode,
                    httpResponse.getEntity()
            );
        }

        if (locationPath != null) {
            expectReplyHeader(httpResponse, "Location", baseURI + fixPath(locationPath));
        }

        if (!httpResponse.getEntity().isPresent()) {
            throw new HttpTestClientException(statusCode, "Expected body in POST reply");
        }
        return httpResponse.getEntity().get();
    }

    private <T> void doPutInternal(String path, T entity) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(baseURI + fixPath(path)).openConnection();
        try {
            connection.setRequestMethod("PUT");
            connection.addRequestProperty("Content-Type", MediaType.APPLICATION_JSON);
            connection.setDoOutput(true);
            MAPPER.writeValue(connection.getOutputStream(), entity);

            expectStatusCode(connection, Status.NO_CONTENT);
        } finally {
            connection.disconnect();
        }
    }

    private void doDeleteInternal(String path) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(baseURI + fixPath(path)).openConnection();
        try {
            connection.setRequestMethod("DELETE");
            expectStatusCode(connection, Status.NO_CONTENT);
        } finally {
            connection.disconnect();
        }
    }

    private String fixPath(String path) {
        return path.isEmpty() || path.startsWith("/") ? path : '/' + path;
    }

    private void expectStatusCode(HttpURLConnection connection, Status... expected) throws IOException {
        int actual = connection.getResponseCode();
        for (Status nextExpected : expected) {
            if (actual == nextExpected.getStatusCode()) {
                return;
            }
        }
        ErrorResponse errorResponse = null;
        try {
            errorResponse = MAPPER.readValue(connection.getInputStream(), ErrorResponse.class);
        } catch (IOException ignore) {
        }
        throw new HttpTestClientException(actual, "Received HTTP status code " + actual + ", but expected one of " + asList(expected), errorResponse);
    }

    private boolean hasExpectedStatusCode(int actual, Status... expected) throws IOException {
        for (Status nextExpected : expected) {
            if (actual == nextExpected.getStatusCode()) {
                return true;
            }
        }
        return false;
    }

    private void expectReplyHeader(HttpURLConnection connection, String headerName, String expected) {
        String actual = connection.getHeaderField(headerName);
        if (actual == null) {
            throw new IllegalStateException("Required " + headerName + " not set; expected value=" + expected);
        }
        if (!actual.equals(expected)) {
            throw new IllegalStateException("Required " + headerName + " set to " + actual + ", not to expected value " + expected);
        }
    }

    private void expectReplyHeader(RxHttpResponse<?> reply, String headerName, String expected) {
        List<String> actual = reply.getHeaders().get(headerName);
        if (CollectionsExt.isNullOrEmpty(actual)) {
            throw new IllegalStateException("Required " + headerName + " not set; expected value=" + expected);
        }
        if (!actual.contains(expected)) {
            throw new IllegalStateException("Required " + headerName + " set to " + actual + ", not to expected value " + expected);
        }
    }

    private void handleErrors(IORunnable request) {
        try {
            request.run();
        } catch (HttpTestClientException e) {
            throw e;
        } catch (RxRestClientException e) {
            throw new HttpTestClientException(e.getStatusCode(), e.getMessage(), e.getErrorBody());
        } catch (Exception e) {
            throw new HttpTestClientException("Unexpected error: " + e.getMessage(), e);
        }
    }

    private <T> T handleErrors(Callable<T> request) {
        try {
            return request.call();
        } catch (HttpTestClientException e) {
            throw e;
        } catch (RxRestClientException e) {
            throw new HttpTestClientException(e.getStatusCode(), e.getMessage(), e.getErrorBody());
        } catch (Exception e) {
            throw new HttpTestClientException("Unexpected error: " + e.getMessage(), e);
        }
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.disable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }

    interface MappingFunction<T> {
        T readValue(InputStream is) throws IOException;
    }

    interface IORunnable {
        void run() throws IOException;
    }
}
