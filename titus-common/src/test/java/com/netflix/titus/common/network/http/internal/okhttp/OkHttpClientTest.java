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

package com.netflix.titus.common.network.http.internal.okhttp;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.netflix.titus.common.network.http.EndpointResolver;
import com.netflix.titus.common.network.http.HttpClient;
import com.netflix.titus.common.network.http.Request;
import com.netflix.titus.common.network.http.RequestBody;
import com.netflix.titus.common.network.http.Response;
import com.netflix.titus.common.network.http.StatusCode;
import com.netflix.titus.common.network.http.internal.RoundRobinEndpointResolver;
import okhttp3.Interceptor;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.tls.HandshakeCertificates;
import okhttp3.tls.HeldCertificate;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OkHttpClientTest {

    private static final String TEST_REQUEST_BODY = "Test Request Body";
    private static final String TEST_RESPONSE_BODY = "Test Response Body";
    private MockWebServer server;

    @Before
    public void setUp() throws Exception {
        server = new MockWebServer();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void testGet() throws Exception {
        MockResponse mockResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.OK.getCode());
        server.enqueue(mockResponse);

        HttpClient client = OkHttpClient.newBuilder()
                .build();

        Request request = new Request.Builder()
                .url(server.url("/").toString())
                .get()
                .build();

        Response response = client.execute(request);
        Assertions.assertThat(response.isSuccessful()).isTrue();

        InputStream inputStream = response.getBody().get(InputStream.class);
        String actualResponseBody = CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
        Assertions.assertThat(actualResponseBody).isEqualTo(TEST_RESPONSE_BODY);

        RecordedRequest recordedRequest = server.takeRequest(1, TimeUnit.MILLISECONDS);
        Assertions.assertThat(recordedRequest).isNotNull();
        Assertions.assertThat(recordedRequest.getBodySize()).isLessThanOrEqualTo(0);
    }

    @Test
    public void testPost() throws Exception {
        MockResponse mockResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.OK.getCode());
        server.enqueue(mockResponse);

        HttpClient client = OkHttpClient.newBuilder()
                .build();

        Request request = new Request.Builder()
                .url(server.url("/").toString())
                .post()
                .body(RequestBody.create(TEST_REQUEST_BODY))
                .build();

        Response response = client.execute(request);
        Assertions.assertThat(response.isSuccessful()).isTrue();

        String actualResponseBody = response.getBody().get(String.class);
        Assertions.assertThat(actualResponseBody).isEqualTo(TEST_RESPONSE_BODY);

        RecordedRequest recordedRequest = server.takeRequest(1, TimeUnit.MILLISECONDS);
        Assertions.assertThat(recordedRequest).isNotNull();
        String actualRequestBody = recordedRequest.getBody().readUtf8();
        Assertions.assertThat(actualRequestBody).isEqualTo(TEST_REQUEST_BODY);
    }

    @Test
    public void testGetWithEndpointResolver() throws Exception {
        MockResponse mockResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.OK.getCode());
        server.enqueue(mockResponse);

        EndpointResolver roundRobinEndpointResolver = new RoundRobinEndpointResolver(server.url("").toString());
        Interceptor endpointResolverInterceptor = new EndpointResolverInterceptor(roundRobinEndpointResolver);
        HttpClient client = OkHttpClient.newBuilder()
                .interceptor(endpointResolverInterceptor)
                .build();

        Request request = new Request.Builder()
                .url("/")
                .get()
                .build();

        Response response = client.execute(request);
        Assertions.assertThat(response.isSuccessful()).isTrue();

        InputStream inputStream = response.getBody().get(InputStream.class);
        String actualResponseBody = CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
        Assertions.assertThat(actualResponseBody).isEqualTo(TEST_RESPONSE_BODY);

        RecordedRequest recordedRequest = server.takeRequest(1, TimeUnit.MILLISECONDS);
        Assertions.assertThat(recordedRequest).isNotNull();
        Assertions.assertThat(recordedRequest.getBodySize()).isLessThanOrEqualTo(0);
    }

    @Test
    public void testGetWithRetries() throws Exception {
        MockResponse notAvailableResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.SERVICE_UNAVAILABLE.getCode());
        server.enqueue(notAvailableResponse);

        MockResponse internalErrorResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.INTERNAL_SERVER_ERROR.getCode());
        server.enqueue(internalErrorResponse);

        MockResponse successfulResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.OK.getCode());
        server.enqueue(successfulResponse);

        Interceptor passthroughInterceptor = new PassthroughInterceptor();
        Interceptor compositeRetryInterceptor = new CompositeRetryInterceptor(Collections.singletonList(passthroughInterceptor), 3);
        HttpClient client = OkHttpClient.newBuilder()
                .interceptor(compositeRetryInterceptor)
                .build();

        Request request = new Request.Builder()
                .url(server.url("/").toString())
                .get()
                .build();

        Response response = client.execute(request);
        Assertions.assertThat(response.isSuccessful()).isTrue();

        InputStream inputStream = response.getBody().get(InputStream.class);
        String actualResponseBody = CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
        Assertions.assertThat(actualResponseBody).isEqualTo(TEST_RESPONSE_BODY);

        server.takeRequest(1, TimeUnit.MILLISECONDS);
        server.takeRequest(1, TimeUnit.MILLISECONDS);
        RecordedRequest recordedRequest = server.takeRequest(1, TimeUnit.MILLISECONDS);
        Assertions.assertThat(recordedRequest).isNotNull();
        Assertions.assertThat(recordedRequest.getBodySize()).isLessThanOrEqualTo(0);
    }

    @Test
    public void testPostWithRetriesSendsOnlyOneRequest() throws Exception {
        MockResponse notAvailableResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.SERVICE_UNAVAILABLE.getCode());
        server.enqueue(notAvailableResponse);

        MockResponse successfulResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.OK.getCode());
        server.enqueue(successfulResponse);

        Interceptor passthroughInterceptor = new PassthroughInterceptor();
        Interceptor compositeRetryInterceptor = new CompositeRetryInterceptor(Collections.singletonList(passthroughInterceptor), 3);
        HttpClient client = OkHttpClient.newBuilder()
                .interceptor(compositeRetryInterceptor)
                .build();

        Request request = new Request.Builder()
                .url(server.url("/").toString())
                .post()
                .body(RequestBody.create(TEST_REQUEST_BODY))
                .build();

        Response response = client.execute(request);
        Assertions.assertThat(server.getRequestCount()).isEqualTo(1);
        Assertions.assertThat(response.getStatusCode()).isEqualTo(StatusCode.SERVICE_UNAVAILABLE);
        Assertions.assertThat(response.getBody().get(String.class)).isEqualTo(TEST_RESPONSE_BODY);
    }

    @Test
    public void testGetWithSslContext() throws Exception {
        String localhost = InetAddress.getByName("localhost").getCanonicalHostName();
        HeldCertificate localhostCertificate = new HeldCertificate.Builder()
                .addSubjectAlternativeName(localhost)
                .build();
        HandshakeCertificates serverCertificates = new HandshakeCertificates.Builder()
                .heldCertificate(localhostCertificate)
                .build();

        MockWebServer sslServer = new MockWebServer();
        sslServer.useHttps(serverCertificates.sslSocketFactory(), false);
        String url = sslServer.url("/").toString();

        MockResponse mockResponse = new MockResponse()
                .setBody(TEST_RESPONSE_BODY)
                .setResponseCode(StatusCode.OK.getCode());
        sslServer.enqueue(mockResponse);

        HandshakeCertificates clientCertificates = new HandshakeCertificates.Builder()
                .addTrustedCertificate(localhostCertificate.certificate())
                .build();
        HttpClient client = OkHttpClient.newBuilder()
                .sslContext(clientCertificates.sslContext())
                .trustManager(clientCertificates.trustManager())
                .build();

        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();

        Response response = client.execute(request);
        Assertions.assertThat(response.isSuccessful()).isTrue();

        InputStream inputStream = response.getBody().get(InputStream.class);
        String actualResponseBody = CharStreams.toString(new InputStreamReader(inputStream, Charsets.UTF_8));
        Assertions.assertThat(actualResponseBody).isEqualTo(TEST_RESPONSE_BODY);

        RecordedRequest recordedRequest = sslServer.takeRequest(1, TimeUnit.MILLISECONDS);
        Assertions.assertThat(recordedRequest).isNotNull();
        Assertions.assertThat(recordedRequest.getBodySize()).isLessThanOrEqualTo(0);
    }
}