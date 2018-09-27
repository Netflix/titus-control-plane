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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.netflix.spectator.api.Id;
import com.netflix.titus.common.network.client.RxHttpResponse;
import com.netflix.titus.common.network.client.RxRestClient;
import com.netflix.titus.common.network.client.RxRestClientException;
import com.netflix.titus.common.util.tuple.Either;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.pipeline.ssl.SSLEngineFactory;
import io.reactivex.netty.protocol.http.client.HttpClient;
import io.reactivex.netty.protocol.http.client.HttpClientBuilder;
import io.reactivex.netty.protocol.http.client.HttpClientPipelineConfigurator;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.subscriptions.Subscriptions;

/**
 * RxNetty 4.x implementation of {@link RxRestClient}.
 */
public class RxNettyRestClient implements RxRestClient {

    private static final Logger logger = LoggerFactory.getLogger(RxNettyRestClient.class);

    /* For testing */ static final ObjectMapper MAPPER = createObjectMapper();

    private final EndpointResolver endpointResolver;
    private final RxClientMetric rxClientMetric;
    private final Optional<Function<Integer, TypeProvider<?>>> errorReplyTypeResolver;
    private final Optional<SSLEngineFactory> sslEngineFactory;

    public RxNettyRestClient(EndpointResolver endpointResolver,
                             RxClientMetric rxClientMetric,
                             Optional<Function<Integer, TypeProvider<?>>> errorReplyTypeResolver,
                             Optional<SSLEngineFactory> sslEngineFactory) {
        this.endpointResolver = endpointResolver;
        this.rxClientMetric = rxClientMetric;
        this.errorReplyTypeResolver = errorReplyTypeResolver;
        this.sslEngineFactory = sslEngineFactory;
    }

    @Override
    public <T> Observable<T> doGET(String relativeURI, TypeProvider<T> type) {
        return doGET(relativeURI, Collections.unmodifiableMap(new HashMap<>()), type);
    }

    @Override
    public <T> Observable<T> doGET(String relativeURI, Map<String, String> headers, TypeProvider<T> type) {
        return Observable.create(subscriber -> {
            long startTime = System.currentTimeMillis();
            doGetInternal(relativeURI, headers, type)
                    .doOnTerminate(() -> registerLatency(HttpMethod.GET, startTime))
                    .subscribe(subscriber);
        });
    }

    @Override
    public <REQ> Observable<Void> doPOST(String relativeURI, REQ entity) {
        return Observable.create(subscriber -> {
            long startTime = System.currentTimeMillis();
            doPostInternal(relativeURI, entity, RxHttpResponseTypeProvider.VOID_TYPE_PROVIDER)
                    .doOnTerminate(() -> registerLatency(HttpMethod.POST, startTime))
                    .subscribe(subscriber);
        });
    }

    @Override
    public <REQ, RESP> Observable<RESP> doPOST(String relativeURI, REQ entity, TypeProvider<RESP> replyTypeProvider) {
        return Observable.create(subscriber -> {
            long startTime = System.currentTimeMillis();
            doPostInternal(relativeURI, entity, replyTypeProvider)
                    .doOnTerminate(() -> registerLatency(HttpMethod.POST, startTime))
                    .subscribe(subscriber);
        });
    }

    @Override
    public <REQ> Observable<Void> doPUT(String relativeURI, REQ entity) {
        return Observable.create(subscriber -> {
            long startTime = System.currentTimeMillis();
            doPutInternal(relativeURI, entity, RxHttpResponseTypeProvider.VOID_TYPE_PROVIDER)
                    .doOnTerminate(() -> registerLatency(HttpMethod.PUT, startTime))
                    .subscribe(subscriber);
        });
    }

    @Override
    public <REQ, RESP> Observable<RESP> doPUT(String relativeURI, REQ entity, TypeProvider<RESP> replyTypeProvider) {
        return Observable.create(subscriber -> {
            long startTime = System.currentTimeMillis();
            doPutInternal(relativeURI, entity, replyTypeProvider)
                    .doOnTerminate(() -> registerLatency(HttpMethod.PUT, startTime))
                    .subscribe(subscriber);
        });
    }

    private <T> Observable<T> doGetInternal(String relativeURI, Map<String, String> headers, TypeProvider<T> type) {
        HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.create(HttpMethod.GET, relativeURI);
        headers.forEach(httpRequest::withHeader);

        return newClient().flatMap(client -> client.submit(httpRequest).doOnTerminate(client::shutdown))
                .flatMap(response -> {
                            if (response.getStatus().code() / 100 != 2) {
                                return readErrorResponse(response)
                                        .flatMap(r -> Observable.error(
                                                new RxRestClientException(response.getStatus().code(), "REST operation failed", r)
                                        ));
                            }
                            return readResponse(response, type)
                                    .doOnNext(next -> incrementRequestCounter(HttpMethod.GET, response));
                        }
                ).doOnError(error -> handleErrorReply(httpRequest, error));
    }

    private <REQ, RESP> Observable<RESP> doPostInternal(String relativeURI, REQ entity, TypeProvider<RESP> replyType) {
        return doSubmitInternal(relativeURI, HttpMethod.POST, entity, replyType);
    }

    private <REQ, RESP> Observable<RESP> doPutInternal(String relativeURI, REQ entity, TypeProvider<RESP> replyType) {
        return doSubmitInternal(relativeURI, HttpMethod.PUT, entity, replyType);
    }

    private <REQ, RESP> Observable<RESP> doSubmitInternal(String relativeURI, HttpMethod httpMethod, REQ entity, TypeProvider<RESP> replyType) {
        String body = null;
        if (entity != null) {
            try {
                body = MAPPER.writeValueAsString(entity);
            } catch (JsonProcessingException e) {
                return Observable.error(new IllegalArgumentException("Cannot serialize provided entity"));
            }
        }

        final HttpClientRequest<ByteBuf> httpRequest = HttpClientRequest.<ByteBuf>create(httpMethod, relativeURI)
                .withHeader(HttpHeaderNames.CONTENT_TYPE.toString(), "application/json");

        if (!Strings.isNullOrEmpty(body)) {
            httpRequest.withContent(Unpooled.wrappedBuffer(body.getBytes()));
        }

        return newClient().flatMap(client -> client.submit(httpRequest).doOnTerminate(client::shutdown))
                .flatMap(response -> {
                            HttpResponseStatus statusCode = response.getStatus();
                            if (statusCode.code() / 100 != 2) {
                                return readErrorResponse(response)
                                        .flatMap(r -> Observable.error(
                                                new RxRestClientException(statusCode.code(), "REST operation failed", r)
                                        ));
                            }

                            if (replyType == RxHttpResponseTypeProvider.VOID_TYPE_PROVIDER) {
                                incrementRequestCounter(httpMethod, response);
                                return Observable.empty();
                            }

                            if (replyType instanceof RxHttpResponseTypeProvider && ((RxHttpResponseTypeProvider) replyType).noBody()) {
                                incrementRequestCounter(httpMethod, response);
                                return Observable.just((RESP) new RxHttpResponse<>(statusCode.code(), copyHeaders(response), Optional.empty()));
                            }

                            return readResponse(response, replyType)
                                    .doOnNext(next -> incrementRequestCounter(httpMethod, response));
                        }
                ).doOnError(error -> handleErrorReply(httpRequest, error));
    }

    private Observable<HttpClient<ByteBuf, ByteBuf>> newClient() {
        return endpointResolver.resolve().map(uri -> {
                    HttpClientBuilder<ByteBuf, ByteBuf> clientBuilder = RxNetty.newHttpClientBuilder(uri.getHost(), uri.getPort());
                    clientBuilder.pipelineConfigurator(new HttpClientPipelineConfigurator<>());
                    if (sslEngineFactory.isPresent()) {
                        clientBuilder.withSslEngineFactory(sslEngineFactory.get());
                    }

                    return clientBuilder.build();
                }
        );
    }

    private <T> Observable<T> readResponse(HttpClientResponse<ByteBuf> response, TypeProvider<T> type) {
        return response.getContent().compose(bodyPartsCollector())
                .flatMap(content -> {
                    try {
                        TypeProvider<?> effectiveType = type instanceof RxHttpResponseTypeProvider
                                ? ((RxHttpResponseTypeProvider) type).getBodyType()
                                : type;

                        Either<Object, Exception> result = parseBody(content, effectiveType);
                        if (result.hasError()) {
                            return Observable.error(new RxRestClientException(response.getStatus().code(), "Could not read response body or map it as type " + effectiveType));
                        }
                        Object entity = result.getValue();

                        if (!(type instanceof RxHttpResponseTypeProvider)) {
                            return Observable.just(entity);
                        }

                        // Wrap body with RxHttpResponse
                        Map<String, List<String>> headers = copyHeaders(response);
                        return Observable.just(new RxHttpResponse<>(response.getStatus().code(), headers, Optional.of(entity)));
                    } finally {
                        content.release();
                    }
                }).map(entity -> (T) entity);
    }

    private <RESP> Observable<Optional<RESP>> readErrorResponse(HttpClientResponse<ByteBuf> response) {
        if (!errorReplyTypeResolver.isPresent()) {
            return Observable.just(Optional.<RESP>empty());
        }
        TypeProvider<?> errorType = errorReplyTypeResolver.get().apply(response.getStatus().code());

        return response.getContent().compose(bodyPartsCollector())
                .flatMap(content -> {
                    Either<Object, Exception> result = parseBody(content, errorType);
                    return result.hasValue() ? Observable.just(Optional.of(result.getValue())) : Observable.just(Optional.empty());
                }).map(entity -> (Optional<RESP>) entity);
    }

    private Map<String, List<String>> copyHeaders(HttpClientResponse<ByteBuf> response) {
        Map<String, List<String>> headers = new HashMap<>();
        for (String name : response.getHeaders().names()) {
            headers.put(name, new ArrayList<>(response.getHeaders().getAll(name)));
        }
        return headers;
    }

    /**
     * Aggregates {@link ByteBuf} chunks into single {@link CompositeByteBuf}.
     */
    private Observable.Transformer<ByteBuf, ByteBuf> bodyPartsCollector() {
        return content -> Observable.create(subscriber -> {
                    CompositeByteBuf collector = Unpooled.compositeBuffer();
                    AtomicBoolean released = new AtomicBoolean(false);
                    Subscription subscription = content.subscribe(
                            next -> {
                                collector.addComponent(true, next);
                                next.retain();
                            },
                            e -> {
                                collector.release();
                                released.set(true);
                                subscriber.onError(e);
                            },
                            () -> {
                                released.set(true); // Release first, as onNext may trigger un-subscribe (see below)
                                subscriber.onNext(collector);
                                subscriber.onCompleted();
                            });
                    subscriber.add(subscription);
                    subscriber.add(Subscriptions.create(() -> {
                        if (!released.get()) {
                            collector.release();
                        }
                    }));
                }
        );
    }

    private Either<Object, Exception> parseBody(ByteBuf byteBufs, TypeProvider<?> type) {
        InputStream is = new ByteBufInputStream(byteBufs);
        try {
            if (type instanceof ClassTypeProvider) {
                return Either.ofValue(MAPPER.readValue(is, type.getEntityClass()));
            } else if (type instanceof JacksonTypeReferenceProvider) {
                return Either.ofValue(MAPPER.readValue(is, ((JacksonTypeReferenceProvider<?>) type).getTypeReference()));
            }
        } catch (IOException e) {
            return Either.ofError(e);
        }
        return Either.ofError(new IllegalArgumentException("Cannot handle entity of type " + type));
    }

    private void handleErrorReply(HttpClientRequest<ByteBuf> httpRequest, Throwable error) {
        HttpMethod method = httpRequest.getMethod();
        String reqSignature = RxRestClientUtil.requestSignature(method.toString(), httpRequest.getAbsoluteUri());

        logger.debug("REST call {} failed with error", reqSignature, error);

        Id id = rxClientMetric.createMethodId(method);
        if (id != null) {
            rxClientMetric.increment(id, error);
        }
    }

    private void incrementRequestCounter(HttpMethod method, HttpClientResponse<ByteBuf> response) {
        Id id = rxClientMetric.createMethodId(method);
        if (id != null) {
            rxClientMetric.increment(id, response.getStatus());
        }
    }

    private void registerLatency(HttpMethod method, long startTime) {
        rxClientMetric.registerLatency(rxClientMetric.createMethodId(method), System.currentTimeMillis() - startTime);
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }
}
