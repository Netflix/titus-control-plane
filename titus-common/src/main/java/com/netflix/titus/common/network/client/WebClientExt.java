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

import java.util.function.Function;

import com.netflix.titus.common.network.client.internal.WebClientMetric;
import com.netflix.titus.common.util.time.Clock;
import org.reactivestreams.Publisher;
import org.springframework.http.HttpMethod;
import reactor.core.publisher.Mono;

public final class WebClientExt {

    /**
     * A {@link Mono} operator that records latency of completion (success or error), use with
     * {@link Mono#compose(Function)}.
     */
    public static <T> Function<Mono<T>, Publisher<T>> latencyMonoOperator(Clock clock, WebClientMetric metrics, HttpMethod method) {
        return source -> new LatencyRecorder<>(clock, source, metrics, method);
    }

    /**
     * A {@link Mono} operator that records latency of completion (success or error), use with
     * {@link Mono#compose(Function)}.
     */
    public static <T> Function<Mono<T>, Publisher<T>> latencyMonoOperator(Clock clock, WebClientMetric metrics, HttpMethod method, String path) {
        return source -> new LatencyRecorder<>(clock, source, metrics, method, path);
    }

}
