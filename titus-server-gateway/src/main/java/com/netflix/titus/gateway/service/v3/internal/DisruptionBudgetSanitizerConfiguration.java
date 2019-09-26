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

package com.netflix.titus.gateway.service.v3.internal;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titusGateway.disruptionBudgetSanitizer")
public interface DisruptionBudgetSanitizerConfiguration {

    long TIME_7DAYS_MS = 7 * 24 * 60 * 60 * 1000;

    /**
     * If set to true, jobs that do not define disruption budget, will get one assigned.
     */
    @DefaultValue("false")
    boolean isEnabled();

    @DefaultValue("" + TIME_7DAYS_MS)
    long getServiceSelfManagedRelocationTimeMs();
}
