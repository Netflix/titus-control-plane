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

package io.netflix.titus.master.scheduler.resourcecache.internal;

import java.util.concurrent.Callable;
import javax.inject.Singleton;

import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;

@Singleton
public class DefaultAgentResourceCache implements AgentResourceCache {

    @Override
    public void createOrUpdateIdle(String instanceId, Callable<AgentResourceCacheInstance> callable) {
    }

    @Override
    public void createOrUpdateActive(String instanceId, Callable<AgentResourceCacheInstance> callable) {
    }

    @Override
    public AgentResourceCacheInstance getIdle(String instanceId) {
        return null;
    }

    @Override
    public AgentResourceCacheInstance getActive(String instanceId) {
        return null;
    }
}
