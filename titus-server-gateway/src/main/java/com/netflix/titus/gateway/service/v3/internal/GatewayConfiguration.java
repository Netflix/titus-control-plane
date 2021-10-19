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

@Configuration(prefix = "titusGateway")
public interface GatewayConfiguration {

    /**
     * Maximum number of tasks in a page. This limit exists due to the GRPC buffer limits.
     */
    @DefaultValue("5000")
    int getMaxTaskPageSize();

    /**
     * If the cache staleness is above this threshold, it will not be used for query requests.
     */
    @DefaultValue("5000")
    long getMaxAcceptableCacheStalenessMs();

    /**
     * If the cache staleness is above this threshold, disconnect all observeJob(s) sourced from this cache.
     */
    @DefaultValue("5000")
    long getObserveJobsStalenessDisconnectMs();

    /**
     * Configure callers whose queries should be handled from the local cache.
     */
    @DefaultValue("NONE")
    String getQueryFromCacheCallerId();
}
