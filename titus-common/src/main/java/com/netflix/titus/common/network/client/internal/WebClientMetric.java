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

import java.time.Duration;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.network.client.ClientMetrics;
import com.netflix.titus.common.util.time.Clock;
import org.springframework.http.HttpMethod;
import reactor.netty.Connection;
import reactor.netty.http.client.HttpClientResponse;

public class WebClientMetric {
    private static final String WEB_CLIENT_METRICS = "titus.webClient.";

    private final ClientMetrics delegate;

    public WebClientMetric(String endpointName, Registry registry, Clock clock) {
        delegate = new ClientMetrics(WEB_CLIENT_METRICS, endpointName, registry, clock);
    }

    public void registerOnSuccessLatency(HttpMethod method, Duration elapsed) {
        delegate.registerOnSuccessLatency(method.name(), elapsed);
    }

    public void registerOnSuccessLatency(HttpMethod method, String path, Duration elapsed) {
        delegate.registerOnSuccessLatency(method.name(), path, elapsed);
    }

    public void registerOnErrorLatency(HttpMethod method, Duration elapsed) {
        delegate.registerOnErrorLatency(method.name(), elapsed);
    }

    public void registerOnErrorLatency(HttpMethod method, String path, Duration elapsed) {
        delegate.registerOnErrorLatency(method.name(), path, elapsed);
    }

    public void incrementOnSuccess(HttpClientResponse response, Connection connection) {
        delegate.incrementOnSuccess(response.method().name(), response.path(), response.status().toString());
    }

    public void incrementOnError(HttpClientResponse response, Throwable throwable) {
        delegate.incrementOnError(response.method().name(), response.path(), throwable);
    }
}
