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

package com.netflix.titus.master.mesos.kubeapiserver;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.network.client.ClientMetrics;
import com.netflix.titus.common.util.time.Clock;
import com.squareup.okhttp.Interceptor;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;

public class OkHttpMetricsInterceptor implements Interceptor {

    private final String metricNamePrefix;
    private final Registry registry;
    private final Clock clock;
    private final Function<Request, String> uriMapper;

    private final ConcurrentMap<String, ClientMetrics> clientMetrics = new ConcurrentHashMap<>();

    public OkHttpMetricsInterceptor(String metricNamePrefix, Registry registry, Clock clock, Function<Request, String> uriMapper) {
        this.metricNamePrefix = metricNamePrefix;
        this.registry = registry;
        this.clock = clock;
        this.uriMapper = uriMapper;
    }

    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();

        String method = request.method();
        String uri = getUri(request);

        ClientMetrics clientMetrics = this.clientMetrics.computeIfAbsent(uri, k -> new ClientMetrics(this.metricNamePrefix, uri, registry));
        long startTimeMs = clock.wallTime();
        try {
            Response response = chain.proceed(request);
            clientMetrics.incrementOnSuccess(method, uri, String.valueOf(response.code()));
            clientMetrics.registerOnSuccessLatency(method, uri, Duration.ofMillis(clock.wallTime() - startTimeMs));
            return response;
        } catch (Exception e) {
            clientMetrics.incrementOnError(method, uri, e);
            clientMetrics.registerOnErrorLatency(method, uri, Duration.ofMillis(clock.wallTime() - startTimeMs));
            throw e;
        }
    }

    private String getUri(Request request) {
        try {
            return uriMapper.apply(request);
        } catch (Exception ignore) {
        }
        return "unknown";
    }
}
