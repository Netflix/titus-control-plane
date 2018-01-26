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

package io.netflix.titus.master.scheduler.resourcecache;

import java.util.Optional;
import java.util.function.Function;

/**
 * Provides a way to access agent resource information in order to make scheduling decisions.
 */
public interface AgentResourceCache {
    /**
     * Creates or updates the agent resource idle cache instance with the specified callable function.
     *
     * @param hostname - the instance's hostname
     * @param function - a function that returns the computed {@link AgentResourceCacheInstance}
     */
    void createOrUpdateIdle(String hostname, Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> function);

    /**
     * Creates or updates the agent resource active cache instance with the specified function function.
     *
     * @param hostname -  the instance's hostname
     * @param function - a function that returns the computed {@link AgentResourceCacheInstance}
     */
    void createOrUpdateActive(String hostname, Function<Optional<AgentResourceCacheInstance>, AgentResourceCacheInstance> function);

    /**
     * Returns the {@link AgentResourceCacheInstance} for a given instance id in the agent resource idle cache or null if it does not exist.
     *
     * @param hostname -  the instance's hostname
     * @return the {@link AgentResourceCacheInstance} in an optional or empty if it does not exist.
     */
    Optional<AgentResourceCacheInstance> getIdle(String hostname);

    /**
     * Returns the {@link AgentResourceCacheInstance} for a given instance id in the agent resource active cache or null if it does not exist.
     *
     * @param hostname -  the instance's hostname
     * @return the {@link AgentResourceCacheInstance} in an optional or empty if it does not exist.
     */
    Optional<AgentResourceCacheInstance> getActive(String hostname);
}
