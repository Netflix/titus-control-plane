package com.netflix.titus.master.service.management;

import java.util.Map;

import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;

public class CapacityManagementFunctions {
    public static boolean isAvailableToUse(AgentInstanceGroup instanceGroup) {
        if (instanceGroup.getLifecycleStatus().getState() != InstanceGroupLifecycleState.Active &&
                instanceGroup.getLifecycleStatus().getState() != InstanceGroupLifecycleState.PhasedOut) {
            return false;
        }

        Map<String, String> attributes = instanceGroup.getAttributes();
        boolean ignore = Boolean.parseBoolean(attributes.get(CapacityManagementAttributes.IGNORE));
        return !ignore;
    }
}
