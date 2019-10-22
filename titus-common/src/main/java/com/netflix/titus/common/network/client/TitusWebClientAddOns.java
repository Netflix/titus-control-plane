/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.common.network.client;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.net.ssl.SSLException;

import com.netflix.titus.common.network.client.internal.WebClientMetric;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.netty.http.HttpOperations;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;

/**
 * A collection of add-ons for Spring {@link WebClient}.
 * <p>
 */
public final class TitusWebClientAddOns {
    private static final Logger logger = LoggerFactory.getLogger(TitusWebClientAddOns.class);

    private static final Logger requestLogger = LoggerFactory.getLogger("WebClientRequestLogger");

    public static WebClient.Builder addTitusDefaults(WebClient.Builder clientBuilder,
                                                     HttpClient httpClient,
                                                     WebClientMetric webClientMetric) {
        HttpClient updatedHttpClient = addMetricCallbacks(
                addLoggingCallbacks(httpClient),
                webClientMetric
        );

        return clientBuilder.clientConnector(new ReactorClientHttpConnector(updatedHttpClient));
    }

    public static WebClient.Builder addTitusDefaults(WebClient.Builder clientBuilder,
                                                     boolean secure,
                                                     WebClientMetric webClientMetric) {
        HttpClient httpClient = HttpClient.create();
        // SSL
        if (secure) {
            try {
                SslContext sslContext = SslContextBuilder.forClient().build();
                httpClient = httpClient.secure(spec -> spec.sslContext(sslContext));
            } catch (SSLException e) {
                logger.error("Unable configure Docker registry client SSL context: {}", e.getMessage());
                throw new RuntimeException("Error configuring SSL context", e);
            }
        }

        return addTitusDefaults(clientBuilder, httpClient, webClientMetric);
    }

    public static Function<Flux<Throwable>, Flux<?>> retryer(Duration interval,
                                                             int retryLimit,
                                                             Function<Throwable, Boolean> retryPredicate,
                                                             Logger callerLogger) {
        return errors -> Flux.defer(() -> {
            AtomicInteger remaining = new AtomicInteger(retryLimit);
            return errors.flatMap(error -> {
                if (!retryPredicate.apply(error)) {
                    return Flux.error(error);
                }
                if (remaining.get() <= 0) {
                    callerLogger.warn("Retry limit reached. Returning error to the client", error);
                    return Flux.error(error);
                }
                remaining.getAndDecrement();
                logger.info("Retrying failed HTTP request in {}ms", interval.toMillis());

                return Flux.interval(interval).take(1);
            });
        });
    }

    private static HttpClient addLoggingCallbacks(HttpClient httpClient) {
        return httpClient
                .doOnRequestError((request, error) -> requestLogger.info(String.format(
                        "%10s %10s %64s %8s %s", request.method(), "NOT_SENT", request.uri(), 0, error.getMessage()
                )))
                .doOnResponse((response, connection) -> requestLogger.info(String.format(
                        "%10s %10s %64s", response.method(), response.status().reasonPhrase(), buildFullUri(response)
                )))
                .doOnResponseError((response, error) -> requestLogger.info(String.format(
                        "%10s %10s %64s %s", response.method(), response.status().reasonPhrase(), buildFullUri(response), error.getMessage()
                )));
    }

    /**
     * {@link HttpClientResponse#uri()} is incomplete, and contains only URL path. We have to reconstruct full URL
     * from pieces available. It is still not complete, as there is no way to find out if this is plain text or
     * TLS connection.
     */
    private static String buildFullUri(HttpClientResponse response) {
        if (response instanceof HttpOperations) {
            HttpOperations httpOperations = (HttpOperations) response;

            StringBuilder sb = new StringBuilder("http://");

            InetSocketAddress address = httpOperations.address();
            sb.append(address.getHostString());
            sb.append(':').append(address.getPort());

            String path = response.path();
            if (!path.startsWith("/")) {
                sb.append('/');
            }
            sb.append(path);

            return sb.toString();
        }
        return response.path();
    }

    private static HttpClient addMetricCallbacks(HttpClient httpClient, WebClientMetric webClientMetric) {
        return httpClient
                .doAfterResponse(webClientMetric::incrementOnSuccess)
                .doOnResponseError(webClientMetric::incrementOnError);
    }
}
