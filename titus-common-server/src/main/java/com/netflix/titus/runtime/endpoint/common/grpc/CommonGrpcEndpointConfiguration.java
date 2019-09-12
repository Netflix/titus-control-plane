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

package com.netflix.titus.runtime.endpoint.common.grpc;

import com.netflix.archaius.api.annotations.DefaultValue;

public interface CommonGrpcEndpointConfiguration {

    @DefaultValue("7104")
    int getPort();

    /**
     * Graceful shutdown time for GRPC server. If zero, shutdown happens immediately, and all client connections are
     * terminated abruptly.
     */
    @DefaultValue("30000")
    long getShutdownTimeoutMs();

    /**
     * Maximum amount of time a client connection may last. Connections not disconnected by client after this time
     * passes, are closed by server. The clients are expected to reconnect if that happens.
     */
    @DefaultValue("1800000")
    long getMaxConnectionAgeMs();
}
