/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.master.scheduler;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 * Configuration used by the regional failover.
 */
@Configuration(prefix = "titus.scheduler")
public interface SchedulerConfiguration {

    /**
     * An option to enable spreading for service jobs in the critical tier.
     *
     * @return whether or not to prefer spreading for service jobs in the critical tier.
     */
    @DefaultValue("true")
    boolean isCriticalServiceJobSpreadingEnabled();
}