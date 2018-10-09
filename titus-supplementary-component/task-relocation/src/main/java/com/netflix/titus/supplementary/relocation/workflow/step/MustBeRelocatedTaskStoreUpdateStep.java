package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.Map;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;

/**
 * Step at which information about task that must be relocated is persisted in the database.
 */
class MustBeRelocatedTaskStoreUpdateStep {

    void peristChangesInStore(Map<String, TaskRelocationPlan> mustBeRelocatedTasks) {
    }
}
