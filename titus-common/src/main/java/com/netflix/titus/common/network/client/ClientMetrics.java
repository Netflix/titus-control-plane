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
import java.util.concurrent.TimeUnit;

import com.netflix.spectator.api.Id;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.common.util.time.Clock;

public class ClientMetrics {
    private static final String CLIENT_REQUEST = "request";
    private static final String CLIENT_LATENCY = "latency";
    private static final String CLIENT_ENDPOINT_TAG = "endpoint";
    private static final String CLIENT_METHOD_TAG = "method";
    private static final String CLIENT_PATH_TAG = "path";
    private static final String CLIENT_RESPONSE_STATUS_TAG = "status";
    private static final String CLIENT_RESPONSE_STATUS_SUCCESS = "success";
    private static final String CLIENT_RESPONSE_STATUS_FAILURE = "failure";

    private final Registry registry;
    private final Clock clock;

    private final Id requestId;
    private final Id latencyId;

    public ClientMetrics(String metricNamePrefix, String endpointName, Registry registry, Clock clock) {
        this.clock = clock;
        String updatedMetricNamePrefix = StringExt.appendToEndIfMissing(metricNamePrefix, ".");
        this.registry = registry;

        requestId = registry.createId(updatedMetricNamePrefix + CLIENT_REQUEST)
                .withTag(CLIENT_ENDPOINT_TAG, endpointName);
        latencyId = registry.createId(updatedMetricNamePrefix + CLIENT_LATENCY)
                .withTag(CLIENT_ENDPOINT_TAG, endpointName);
    }

    /**
     * Register a success latency metric based on how much time elapsed from when a request was sent.
     */
    public void registerOnSuccessLatency(String methodName, Duration elapsed) {
        recordTimer(latencyId
                        .withTag(CLIENT_METHOD_TAG, methodName)
                        .withTag(CLIENT_RESPONSE_STATUS_TAG, CLIENT_RESPONSE_STATUS_SUCCESS),
                elapsed
        );
    }

    /**
     * Register a success latency metric based on how much time elapsed from when a request was sent.
     */
    public void registerOnSuccessLatency(String methodName, String path, Duration elapsed) {
        recordTimer(latencyId
                        .withTag(CLIENT_METHOD_TAG, methodName)
                        .withTag(CLIENT_PATH_TAG, path)
                        .withTag(CLIENT_RESPONSE_STATUS_TAG, CLIENT_RESPONSE_STATUS_SUCCESS),
                elapsed
        );
    }

    /**
     * Register an error latency metric based on how much time elapsed from when a request was sent.
     */
    public void registerOnErrorLatency(String methodName, Duration elapsed) {
        recordTimer(latencyId
                        .withTag(CLIENT_METHOD_TAG, methodName)
                        .withTag(CLIENT_RESPONSE_STATUS_TAG, CLIENT_RESPONSE_STATUS_FAILURE),
                elapsed
        );
    }

    /**
     * Register an error latency metric based on how much time elapsed from when a request was sent.
     */
    public void registerOnErrorLatency(String methodName, String path, Duration elapsed) {
        recordTimer(latencyId
                        .withTag(CLIENT_METHOD_TAG, methodName)
                        .withTag(CLIENT_PATH_TAG, path)
                        .withTag(CLIENT_RESPONSE_STATUS_TAG, CLIENT_RESPONSE_STATUS_FAILURE),
                elapsed
        );
    }

    private void recordTimer(Id id, Duration elapsed) {
        registry.timer(id).record(elapsed.toMillis(), TimeUnit.MILLISECONDS);
    }

    public void incrementOnSuccess(String methodName, String path, String status) {
        registry.counter(requestId
                .withTag(CLIENT_METHOD_TAG, methodName)
                .withTag(CLIENT_PATH_TAG, path)
                .withTag("statusCode", status)
        ).increment();
    }

    public void incrementOnError(String methodName, String path, Throwable throwable) {
        registry.counter(requestId
                .withTag(CLIENT_METHOD_TAG, methodName)
                .withTag(CLIENT_PATH_TAG, path)
                .withTag("error", throwable.getClass().getSimpleName())
        ).increment();
    }
}
