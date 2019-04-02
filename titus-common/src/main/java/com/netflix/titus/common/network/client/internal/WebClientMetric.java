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

package com.netflix.titus.common.network.client.internal;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import org.springframework.http.HttpMethod;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClientResponse;

public class WebClientMetric {
    private static final String WEB_CLIENT_METRICS = "titus.webClient.";
    private static final String WEB_CLIENT_REQUEST = WEB_CLIENT_METRICS + "request";
    private static final String WEB_CLIENT_LATENCY = WEB_CLIENT_METRICS + "latency";
    private static final String WEB_CLIENT_ENDPOINT_TAG = "endpoint";
    private static final String WEB_CLIENT_METHOD_TAG = "method";
    private static final String WEB_CLIENT_PATH_TAG = "path";

    private final Registry registry;
    private final Id requestId;
    private final Id latencyId;

    public WebClientMetric(String endpointName, Registry registry) {
        this.registry = registry;

        requestId = registry.createId(WEB_CLIENT_REQUEST)
                .withTag(WEB_CLIENT_ENDPOINT_TAG, endpointName);
        latencyId = registry.createId(WEB_CLIENT_LATENCY)
                .withTag(WEB_CLIENT_ENDPOINT_TAG, endpointName);
    }

    public void registerLatency(HttpMethod method, long startTimeMs) {
        registry.timer(latencyId
                .withTag(WEB_CLIENT_METHOD_TAG, method.name()))
                .record(System.currentTimeMillis() - startTimeMs, TimeUnit.MILLISECONDS);
    }

    // Returns a function to increment on success
    public BiConsumer<? super HttpClientResponse, ? super Connection> getIncrementOnSuccess() {
        return (response, connection) -> {
            registry.counter(requestId
                    .withTag(WEB_CLIENT_METHOD_TAG, response.method().name())
                    .withTag(WEB_CLIENT_PATH_TAG, response.path())
                    .withTag("statusCode", response.status().toString())
            ).increment();
        };
    }

    // Returns a function to increment on error
    public BiConsumer<? super HttpClientResponse, ? super Throwable> getIncrementOnError() {
        return (response, throwable) -> {
            registry.counter(requestId
                    .withTag(WEB_CLIENT_METHOD_TAG, response.method().name())
                    .withTag(WEB_CLIENT_PATH_TAG, response.path())
                    .withTag("error", throwable.getClass().getSimpleName())
            ).increment();
        };
    }
}
