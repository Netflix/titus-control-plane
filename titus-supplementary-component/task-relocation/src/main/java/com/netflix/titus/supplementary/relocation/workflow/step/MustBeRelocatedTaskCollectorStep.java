package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.Map;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;

/**
 * Step at which all containers that are requested to terminate are identified, and their relocation timestamps are set.
 */
public class MustBeRelocatedTaskCollectorStep {

    public MustBeRelocatedTaskCollectorStep() {

    }

    public Map<String, TaskRelocationPlan> collectTasksThatMustBeRelocated() {
        return null;
    }
}
