package com.netflix.titus.supplementary.relocation.workflow.step;

import java.util.Map;

import com.netflix.titus.supplementary.relocation.model.TaskRelocationPlan;
import com.netflix.titus.supplementary.relocation.model.TaskRelocationStatus;

/**
 * At this step, the task eviction result is written to the database.
 */
public class TaskEvictionResultStoreStep {

    public void storeTaskEvictionResults(Map<String, TaskRelocationPlan> taskEvictionPlans, Map<String, TaskRelocationStatus> taskEvictionResults) {
    }
}
