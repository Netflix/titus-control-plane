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

package com.netflix.titus.master.scheduler.resourcecache;

import java.util.Optional;
import java.util.function.Function;

/**
 * Provides a way to access agent resource information in order to make scheduling decisions.
 */
public interface AgentResourceCache {
    /**
     * Creates or updates the agent resource cache instance with the specified function.
     *
     * @param hostname the instance's hostname
     * @param function a function that returns the computed {@link AgentResourceCacheInstance}
     */
    void createOrUpdate(String hostname, Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> function);

    /**
     * Returns {@link Optional<AgentResourceCacheInstance>} for a given instance id in the agent resource cache.
     *
     * @param hostname the instance's hostname
     * @return {@link Optional<AgentResourceCacheInstance>} for a given instance id in the agent resource cache
     */
    Optional<AgentResourceCacheInstance> get(String hostname);

    /**
     * Shuts down the cache
     */
    void shutdown();
}
