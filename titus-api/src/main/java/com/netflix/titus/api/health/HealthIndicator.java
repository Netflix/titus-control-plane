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

package com.netflix.titus.api.health;

import java.util.Map;

import reactor.core.publisher.Flux;

/**
 * Health API similar to Netflix https://github.com/Netflix/runtime-health. Unlike the latter it provides
 * synchronous health evaluator together with event stream to listen for changes.
 */
public interface HealthIndicator {

    Map<String, HealthStatus> getComponents();

    HealthStatus health();

    /**
     * We do not need the event stream yet, but it is provided for the API completeness.
     */
    default Flux<HealthStatus> healthChangeEvents() {
        throw new IllegalStateException("not implemented");
    }
}
