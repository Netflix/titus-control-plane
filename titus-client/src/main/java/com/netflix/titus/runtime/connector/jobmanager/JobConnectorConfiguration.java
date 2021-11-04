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

package com.netflix.titus.runtime.connector.jobmanager;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.connector.jobService")
public interface JobConnectorConfiguration {

    /**
     * Set to true to enable connection timeout if the first event is not emitted in the configured amount of time.
     */
    @DefaultValue("false")
    boolean isConnectionTimeoutEnabled();

    /**
     * See {@link #isConnectionTimeoutEnabled()}.
     */
    @DefaultValue("30000")
    long getConnectionTimeoutMs();

    /**
     * Enable {@link JobDataReplicator} with the server side keep alive mechanism. Use only in TitusGateway.
     */
    @DefaultValue("false")
    boolean isKeepAliveReplicatedStreamEnabled();

    /**
     * Internal at which the keep alive requests are sent over the GRPC channel.
     */
    @DefaultValue("100")
    long getKeepAliveIntervalMs();
}
