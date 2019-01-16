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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.net.ssl.SSLException;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.runtime.TitusRuntime;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;

/**
 * A collection of add-ons for Spring {@link WebClient}.
 * <p>
 * TODO Request latency logging, metrics
 */
public final class TitusWebClientAddOns {

    private static final String WEB_CLIENT_METRICS = "titus.webClient.";
    private static final String WEB_CLIENT_REQUEST = WEB_CLIENT_METRICS + "request";

    private static final Logger logger = LoggerFactory.getLogger(TitusWebClientAddOns.class);

    private static final Logger requestLogger = LoggerFactory.getLogger("WebClientRequestLogger");


    public static WebClient.Builder addTitusDefaults(WebClient.Builder clientBuilder,
                                                     String endpointName,
                                                     boolean secure,
                                                     TitusRuntime titusRuntime) {

        HttpClient httpClient = addMetricCallbacks(
                addLoggingCallbacks(HttpClient.create()),
                endpointName,
                titusRuntime.getRegistry()
        );

        // SSL
        if (secure) {
            try {
                SslContext sslContext = SslContextBuilder.forClient().build();
                httpClient.secure(spec -> spec.sslContext(sslContext));
            } catch (SSLException e) {
                logger.error("Unable configure Docker registry client SSL context: {}", e);
                throw new RuntimeException("Error configuring SSL context", e);
            }
        }

        return clientBuilder.clientConnector(new ReactorClientHttpConnector(httpClient));
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
                logger.info("Retrying failed HTTP request in %sms", interval.toMillis());

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
                        "%10s %10s %64s", response.method(), response.status().reasonPhrase(), response.uri()
                )))
                .doOnResponseError((response, error) -> requestLogger.info(String.format(
                        "%10s %10s %64s %s", response.method(), response.status().reasonPhrase(), response.uri(), error.getMessage()
                )));
    }

    private static HttpClient addMetricCallbacks(HttpClient httpClient, String endpointName, Registry registry) {
        return httpClient
                .doOnResponse((response, connection) -> {
                    registry.counter(WEB_CLIENT_REQUEST,
                            "endpoint", endpointName,
                            "method", response.method().name(),
                            "path", response.path()
                    ).increment();
                })
                .doOnResponseError((response, error) -> {
                    registry.counter(WEB_CLIENT_REQUEST,
                            "endpoint", endpointName,
                            "method", response.method().name(),
                            "path", response.path(),
                            "error", error.getClass().getSimpleName()
                    ).increment();
                });
    }
}
