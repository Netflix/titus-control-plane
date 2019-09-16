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

import java.util.Collections;
import java.util.Map;

public final class HealthIndicators {

    private static final HealthStatus HEALTH_STATUS = HealthStatus.newBuilder()
            .withHealthState(HealthState.Healthy)
            .withDetails(Collections.emptyMap())
            .build();

    private static final Map<String, HealthStatus> COMPONENTS = Collections.singletonMap("alwaysHealthy", HEALTH_STATUS);

    private static final HealthIndicator ALWAYS_HEALTHY_INDICATOR = new HealthIndicator() {
        @Override
        public Map<String, HealthStatus> getComponents() {
            return COMPONENTS;
        }

        @Override
        public HealthStatus health() {
            return HEALTH_STATUS;
        }
    };

    private HealthIndicators() {
    }

    public static HealthIndicator alwaysHealthy() {
        return ALWAYS_HEALTHY_INDICATOR;
    }
}
