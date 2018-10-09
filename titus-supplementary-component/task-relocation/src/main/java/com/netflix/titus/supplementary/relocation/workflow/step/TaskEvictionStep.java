package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.Map;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;

/**
 * In this step, all tasks that were selected for termination, are terminated.
 */
class TaskEvictionStep {

    Map<String, TaskRelocationStatus> evict(Map<String, TaskRelocationPlan> taskToEvict) {
        return null;
    }
}
