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

package com.netflix.titus.common.util.rx;

import java.time.Duration;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;

public class ReactorRetriers {

    public static <T> Function<Flux<T>, Publisher<T>> instrumentedRetryer(String name, Duration retryInterval, Logger logger) {
        return source -> source.retryWhen(errors ->
                errors.flatMap(error -> {
                    logger.warn("Retrying subscription for {} after {}ms due to an error: error={}", name, retryInterval.toMillis(), error.getMessage());
                    return Flux.interval(retryInterval).take(1);
                }));
    }
}
