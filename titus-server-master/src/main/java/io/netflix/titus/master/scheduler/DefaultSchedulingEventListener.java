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

package io.netflix.titus.master.scheduler;

import com.netflix.fenzo.SchedulingEventListener;
import com.netflix.fenzo.TaskAssignmentResult;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheFunctions;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;

public class DefaultSchedulingEventListener implements SchedulingEventListener {

    private final TitusRuntime titusRuntime;
    private final AgentResourceCache agentResourceCache;

    public DefaultSchedulingEventListener(TitusRuntime titusRuntime, AgentResourceCache agentResourceCache) {
        this.titusRuntime = titusRuntime;
        this.agentResourceCache = agentResourceCache;
    }

    @Override
    public void onScheduleStart() {
    }

    @Override
    public void onAssignment(TaskAssignmentResult taskAssignmentResult) {
        String hostname = taskAssignmentResult.getHostname();
        AgentResourceCacheInstance instance = AgentResourceCacheFunctions.createInstance(hostname, taskAssignmentResult,
                titusRuntime.getClock().wallTime());
        agentResourceCache.createOrUpdateIdle(hostname, instanceOpt -> {
            if (instanceOpt.isPresent()) {
                return AgentResourceCacheFunctions.mergeInstances(instanceOpt.get(), instance);
            }
            return instance;
        });
    }

    @Override
    public void onScheduleFinish() {
    }
}
