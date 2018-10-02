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

package com.netflix.titus.common.network.client.internal;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.network.client.RxHttpResponse;
import com.netflix.titus.common.network.client.RxRestClientException;
import com.netflix.titus.common.network.client.TypeProviders;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.ssl.DefaultFactories;
import io.reactivex.netty.pipeline.ssl.SSLEngineFactory;
import io.reactivex.netty.protocol.http.server.HttpServer;
import io.reactivex.netty.protocol.http.server.HttpServerBuilder;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.RequestHandler;
import org.junit.Test;
import rx.Observable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

public class RxNettyRestClientTest {

    private static final TypeReference<List<MyEntity>> MY_ENTITY_LIST_REF = new TypeReference<List<MyEntity>>() {
    };
    public static final int _100_KB = 100 * 1024;

    private final RxClientMetric clientMetrics = new RxClientMetric("testClient", new DefaultRegistry());
    private HttpServer<ByteBuf, ByteBuf> httpServer;
    private RxNettyRestClient client;

    @Test(timeout = 30_000)
    public void testGET() throws Exception {
        MyEntity actual = new MyEntity("test");

        RequestHandler<ByteBuf, ByteBuf> requestHandler = (request, response) -> {
            response.setStatus(HttpResponseStatus.OK);
            return response.writeAndFlush(toByteBuf(actual));
        };

        runHttpAndHttps(requestHandler, () -> {
            MyEntity result = client.doGET("/path", TypeProviders.of(MyEntity.class)).toBlocking().first();
            assertThat(result).isEqualTo(actual);
        });
    }

    @Test(timeout = 30_000)
    public void testGetWithWrapper() throws Exception {
        MyEntity actual = new MyEntity("test");

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> {
            response.setStatus(HttpResponseStatus.OK);
            return response.writeAndFlush(toByteBuf(actual));
        };

        runHttpAndHttps(requesterHandler, () -> {
            RxHttpResponse<MyEntity> result = client.doGET("/path", TypeProviders.ofResponse(MyEntity.class)).toBlocking().first();
            assertThat(result.getEntity()).contains(actual);
        });
    }

    @Test(timeout = 30_000)
    public void testGetNotFound() throws Exception {
        MyErrorReply serverError = new MyErrorReply("simulated error");

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return response.writeBytesAndFlush(toJSON(serverError).getBytes());
        };

        runHttpAndHttps(requesterHandler, () -> {
            try {
                client.doGET("/path", TypeProviders.of(MyEntity.class)).toBlocking().first();
                fail("Expected to fail");
            } catch (RxRestClientException e) {
                assertThat(e.getStatusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                assertThat(e.getErrorBody()).contains(serverError);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testVeryLongGET() throws Exception {
        char[] cbuf = new char[_100_KB];
        Arrays.fill(cbuf, 'A');
        String veryLongMessage = new String(cbuf);

        MyEntity actual = new MyEntity(veryLongMessage);

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> {
            response.setStatus(HttpResponseStatus.OK);
            return response.writeAndFlush(toByteBuf(actual));
        };

        runHttpAndHttps(requesterHandler, () -> {
            MyEntity result = client.doGET("/path", TypeProviders.of(MyEntity.class)).toBlocking().first();
            assertThat(result).isEqualTo(actual);
        });
    }

    @Test(timeout = 30_000)
    public void testGetWithHeader() throws Exception {
        MyEntity actual = new MyEntity("test");
        final String requestHeaderKey = "requestKey";
        final String requestHeaderValue = "requestValue";
        final String responseHeaderKey = "responseKey";
        final String responseHeaderValue = "responseValue";

        RequestHandler<ByteBuf, ByteBuf> requestHandler = (request, response) -> {
            if (request.getHeaders().contains(requestHeaderKey) &&
                    request.getHeaders().get(requestHeaderKey).equals(requestHeaderValue)) {
                response.setStatus(HttpResponseStatus.OK);
            } else {
                response.setStatus(HttpResponseStatus.BAD_REQUEST);
            }
            response.getHeaders().addHeader(responseHeaderKey, responseHeaderValue);
            return response.writeAndFlush(toByteBuf(actual));
        };

        Map<String, String> headers = new HashMap<String, String>();
        headers.put(requestHeaderKey, requestHeaderValue);
        runHttpAndHttps(requestHandler, () -> {
            RxHttpResponse result = client.doGET("/path", headers, TypeProviders.ofEmptyResponse()).toBlocking().first();
            assertThat(result.getStatusCode()).isEqualTo(HttpResponseStatus.OK.code());
            assertThat(result.getHeaders().get(responseHeaderKey)).isEqualTo(Collections.singletonList(responseHeaderValue));
        });
    }

    @Test(timeout = 30_000)
    public void testGetCollection() throws Exception {
        List<MyEntity> actual = Arrays.asList(new MyEntity("item1"), new MyEntity("item2"));

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> {
            response.setStatus(HttpResponseStatus.OK);
            return response.writeAndFlush(toByteBuf(actual));
        };

        runHttpAndHttps(requesterHandler, () -> {
            List<MyEntity> result = client.doGET("/path", TypeProviders.of(MY_ENTITY_LIST_REF)).toBlocking().first();
            assertThat(result).isEqualTo(actual);
        });
    }

    @Test(timeout = 30_000)
    public void testPostWithEmptyBodyReply() throws Exception {
        AtomicReference<MyEntity> valueCapture = new AtomicReference<>();

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> readRequestBody(request, MyEntity.class).flatMap(entity -> {
            response.setStatus(HttpResponseStatus.NO_CONTENT);
            valueCapture.set(entity);
            return Observable.<Void>empty();
        });

        runHttpAndHttps(requesterHandler, () -> {
            client.doPOST("/path", new MyEntity("test")).toBlocking().firstOrDefault(null);
            assertThat(valueCapture.get()).isNotNull();
        });
    }

    @Test(timeout = 30_000)
    public void testPostWithBodyReply() throws Exception {
        AtomicReference<MyEntity> valueCapture = new AtomicReference<>();

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> readRequestBody(request, MyEntity.class).flatMap(entity -> {
            response.setStatus(HttpResponseStatus.OK);
            valueCapture.set(entity);

            String reply = toJSON(new MyEntity(entity.getMessage().toUpperCase()));
            response.writeBytesAndFlush(reply.getBytes());
            return Observable.<Void>empty();
        });

        runHttpAndHttps(requesterHandler, () -> {
            MyEntity result = client.doPOST("/path", new MyEntity("test"), TypeProviders.of(MyEntity.class)).toBlocking().first();

            assertThat(valueCapture.get()).isNotNull();
            assertThat(result.getMessage()).isEqualTo("TEST");
        });
    }

    @Test(timeout = 30_000)
    public void testPostBodyWithErrorReply() throws Exception {
        MyErrorReply serverError = new MyErrorReply("simulated error");

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> readRequestBody(request, MyEntity.class).flatMap(entity -> {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return response.writeBytesAndFlush(toJSON(serverError).getBytes());
        });

        runHttpAndHttps(requesterHandler, () -> {
            try {
                client.doPOST("/path", new MyEntity("test"), TypeProviders.of(MyEntity.class)).toBlocking().first();
                fail("Expected to fail");
            } catch (RxRestClientException e) {
                assertThat(e.getStatusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                assertThat(e.getErrorBody()).contains(serverError);
            }
        });
    }

    @Test(timeout = 30_000)
    public void testPutWithEmptyBodyReply() throws Exception {
        AtomicReference<MyEntity> valueCapture = new AtomicReference<>();

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> readRequestBody(request, MyEntity.class).flatMap(entity -> {
            response.setStatus(HttpResponseStatus.NO_CONTENT);
            valueCapture.set(entity);
            return Observable.<Void>empty();
        });

        runHttpAndHttps(requesterHandler, () -> {
            client.doPUT("/path", new MyEntity("test")).toBlocking().firstOrDefault(null);
            assertThat(valueCapture.get()).isNotNull();
        });
    }

    @Test(timeout = 30_000)
    public void testPutWithEmptyBody() throws Exception {

        AtomicReference<ByteBuf> valueCapture = new AtomicReference<>();

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> request.getContent()
                .take(1).flatMap(byteBuf -> {
                    response.setStatus(HttpResponseStatus.NO_CONTENT);
                    valueCapture.set(byteBuf);
                    return Observable.<Void>empty();
                });

        runHttpAndHttps(requesterHandler, () -> {
            client.doPUT("/path", null).toBlocking().firstOrDefault(null);
            assertThat(valueCapture.get().capacity()).isEqualTo(0);
        });
    }

    @Test(timeout = 30_000)
    public void testPutWithBodyReply() throws Exception {
        AtomicReference<MyEntity> valueCapture = new AtomicReference<>();

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> readRequestBody(request, MyEntity.class).flatMap(entity -> {
            response.setStatus(HttpResponseStatus.OK);
            valueCapture.set(entity);

            String reply = toJSON(new MyEntity(entity.getMessage().toUpperCase()));
            response.writeBytesAndFlush(reply.getBytes());
            return Observable.<Void>empty();
        });

        runHttpAndHttps(requesterHandler, () -> {
            MyEntity result = client.doPUT("/path", new MyEntity("test"), TypeProviders.of(MyEntity.class)).toBlocking().first();

            assertThat(valueCapture.get()).isNotNull();
            assertThat(result.getMessage()).isEqualTo("TEST");
        });
    }

    @Test(timeout = 30_000)
    public void testPutBodyWithErrorReply() throws Exception {
        MyErrorReply serverError = new MyErrorReply("simulated error");

        RequestHandler<ByteBuf, ByteBuf> requesterHandler = (request, response) -> readRequestBody(request, MyEntity.class).flatMap(entity -> {
            response.setStatus(HttpResponseStatus.NOT_FOUND);
            return response.writeBytesAndFlush(toJSON(serverError).getBytes());
        });

        runHttpAndHttps(requesterHandler, () -> {
            try {
                client.doPUT("/path", new MyEntity("test"), TypeProviders.of(MyEntity.class)).toBlocking().first();
                fail("Expected to fail");
            } catch (RxRestClientException e) {
                assertThat(e.getStatusCode()).isEqualTo(HttpResponseStatus.NOT_FOUND.code());
                assertThat(e.getErrorBody()).contains(serverError);
            }
        });
    }

    private <T> Observable<T> readRequestBody(HttpServerRequest<ByteBuf> request, Class<T> entityType) {
        return request.getContent().take(1).flatMap(content -> {
            InputStream is = new ByteBufInputStream(content);
            T entity;
            try {
                entity = RxNettyRestClient.MAPPER.readValue((InputStream) is, entityType);
            } catch (IOException e) {
                return Observable.error(e);
            }
            return Observable.just(entity);
        });
    }

    private void setupServer(RequestHandler<ByteBuf, ByteBuf> requestHandler, boolean secure) throws Exception {
        HttpServerBuilder<ByteBuf, ByteBuf> serverBuilder = RxNetty.newHttpServerBuilder(0, requestHandler);
        if (secure) {
            serverBuilder.withSslEngineFactory(DefaultFactories.selfSigned());
        }

        this.httpServer = serverBuilder.build();
        httpServer.start();

        String scheme = secure ? "https" : "http";
        URI baseUri = new URI(scheme + "://localhost:" + httpServer.getServerPort());

        Optional<SSLEngineFactory> sslEngineFactoryOptional = secure ? Optional.of(DefaultFactories.trustAll()) : Optional.empty();

        this.client = new RxNettyRestClient(() -> Observable.just(baseUri), clientMetrics,
                Optional.of(status -> TypeProviders.of(MyErrorReply.class)), sslEngineFactoryOptional);
    }

    private void runHttpAndHttps(RequestHandler<ByteBuf, ByteBuf> requestHandler, Runnable r) throws Exception {
        boolean[] secureValues = {false, true};
        for (boolean secure : secureValues) {
            try {
                setupServer(requestHandler, secure);
                r.run();
            } finally {
                if (httpServer != null) {
                    httpServer.shutdown();
                }
            }
        }
    }

    private ByteBuf toByteBuf(Object entity) {
        String text;
        try {
            text = RxNettyRestClient.MAPPER.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
        return UnpooledByteBufAllocator.DEFAULT.buffer().writeBytes(text.getBytes(Charset.defaultCharset()));
    }

    private <T> String toJSON(T entity) {
        try {
            return RxNettyRestClient.MAPPER.writeValueAsString(entity);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unexpected error", e);
        }
    }

    static class MyEntity {

        private final String message;

        @JsonCreator
        public MyEntity(@JsonProperty("message") String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyEntity myEntity = (MyEntity) o;

            return message != null ? message.equals(myEntity.message) : myEntity.message == null;

        }

        @Override
        public int hashCode() {
            return message != null ? message.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "MyEntity{" +
                    "message='" + message + '\'' +
                    '}';
        }
    }

    static class MyErrorReply {
        private final String errorMessage;

        @JsonCreator
        public MyErrorReply(@JsonProperty("errorMessage") String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MyErrorReply that = (MyErrorReply) o;

            return errorMessage != null ? errorMessage.equals(that.errorMessage) : that.errorMessage == null;

        }

        @Override
        public int hashCode() {
            return errorMessage != null ? errorMessage.hashCode() : 0;
        }
    }
}