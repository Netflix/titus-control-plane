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

package com.netflix.titus.runtime.connector;

import com.netflix.archaius.api.annotations.DefaultValue;

public interface ChannelTunablesConfiguration {

    long DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
    long DEFAULT_STREAMING_TIMEOUT_MS = 30 * 60_000;

    /**
     * GRPC operation timeout.
     */
    @DefaultValue("" + DEFAULT_REQUEST_TIMEOUT_MS)
    long getRequestTimeoutMs();

    /**
     * Event streams have unbounded lifetime, but we want to terminate them periodically to improve request distribution
     * across multiple nodes.
     */
    @DefaultValue("" + DEFAULT_STREAMING_TIMEOUT_MS)
    long getStreamingTimeoutMs();
}
