/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master;

import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.archaius.api.annotations.PropertyName;

/**
 * The configurations declared in this interface should be the ones that are shared by both worker and master (and
 * potentially other sub-projects).
 */
public interface CoreConfiguration {
    @PropertyName(name = "titus.zookeeper.connectionTimeMs")
    @DefaultValue("10000")
    int getZkConnectionTimeoutMs();

    @PropertyName(name = "titus.zookeeper.connection.retrySleepMs")
    @DefaultValue("500")
    int getZkConnectionRetrySleepMs();

    @PropertyName(name = "titus.zookeeper.connection.retryCount")
    @DefaultValue("5")
    int getZkConnectionMaxRetries();

    @PropertyName(name = "titus.zookeeper.connectString")
    @DefaultValue("localhost:2181")
    String getZkConnectionString();

    @PropertyName(name = "titus.zookeeper.root")
    String getZkRoot();

    @PropertyName(name = "titus.localmode")
    @DefaultValue("false")
    boolean isLocalMode();

    @PropertyName(name = "titus.metricsPublisher.publishFrequencyInSeconds")
    @DefaultValue("15")
    int getMetricsPublisherFrequencyInSeconds();
}
