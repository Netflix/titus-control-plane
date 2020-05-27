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

package com.netflix.titus.master.eviction.service;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titusMaster.eviction")
public interface EvictionServiceConfiguration {

    @DefaultValue("100")
    long getEventStreamQuotaUpdateIntervalMs();

    /**
     * The queue size for pending task termination requests. Incoming requests above this limit are rejected.
     * The queue depth should be equal to at least the system disruption budget capacity.
     */
    @DefaultValue("200")
    int getTerminationQueueSize();
}
