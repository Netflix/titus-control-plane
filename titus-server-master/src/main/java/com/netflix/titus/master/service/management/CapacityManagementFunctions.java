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

package com.netflix.titus.master.service.management;

import java.util.Map;

import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.service.AgentManagementService;

public class CapacityManagementFunctions {

    public static boolean isAvailableToUse(AgentManagementService agentManagementService,
                                           AgentInstanceGroup instanceGroup) {
        if (!agentManagementService.isOwnedByFenzo(instanceGroup)) {
            return false;
        }
        if (instanceGroup.getLifecycleStatus().getState() != InstanceGroupLifecycleState.Active &&
                instanceGroup.getLifecycleStatus().getState() != InstanceGroupLifecycleState.PhasedOut) {
            return false;
        }

        Map<String, String> attributes = instanceGroup.getAttributes();
        boolean ignore = Boolean.parseBoolean(attributes.get(CapacityManagementAttributes.IGNORE));
        return !ignore;
    }
}
