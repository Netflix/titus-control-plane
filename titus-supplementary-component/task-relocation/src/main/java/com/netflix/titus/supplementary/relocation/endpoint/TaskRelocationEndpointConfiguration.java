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

package com.netflix.titus.supplementary.relocation.endpoint;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.relocation.endpoint")
public interface TaskRelocationEndpointConfiguration {

    @DefaultValue("7104")
    int getGrpcPort();

    /**
     * Graceful shutdown time for GRPC server. If zero, shutdown happens immediately, and all client connections are
     * terminated abruptly.
     */
    @DefaultValue("30000")
    long getShutdownTimeoutMs();

    @DefaultValue("1800000")
    long getMaxConnectionAgeMs();
}
