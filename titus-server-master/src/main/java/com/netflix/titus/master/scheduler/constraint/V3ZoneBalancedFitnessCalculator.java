package com.netflix.titus.master.scheduler.constraint;

import java.util.Map;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.master.scheduler.SchedulerUtils;

public class V3ZoneBalancedFitnessCalculator implements VMTaskFitnessCalculator {

    private final TaskCache taskCache;
    private final int expectedValues;
    private final String zoneAttributeName;

    public V3ZoneBalancedFitnessCalculator(TaskCache taskCache, int expectedValues, String zoneAttributeName) {
        this.taskCache = taskCache;
        this.expectedValues = expectedValues;
        this.zoneAttributeName = zoneAttributeName;
    }

    @Override
    public String getName() {
        return "V3ZoneBalancedFitnessCalculator";
    }

    @Override
    public double calculateFitness(TaskRequest taskRequest, VirtualMachineCurrentState targetVM, TaskTrackerState taskTrackerState) {
        // Ignore the constraint for non-V3 tasks.
        if (!(taskRequest instanceof V3QueueableTask)) {
            return 1.0;
        }

        String targetZoneId = SchedulerUtils.getZoneId(targetVM, zoneAttributeName);
        if (targetZoneId == null) {
            return 0.0;
        }

        V3QueueableTask v3FenzoTask = (V3QueueableTask) taskRequest;
        Map<String, Integer> tasksByZoneId = SchedulerUtils.groupCurrentlyAssignedTasksByZoneId(v3FenzoTask.getJob().getId(), taskTrackerState.getAllCurrentlyAssignedTasks().values(), zoneAttributeName);
        Map<String, Integer> runningTasksByZoneId = taskCache.getTasksByZoneIdCounters(v3FenzoTask.getJob().getId());
        for (Map.Entry<String, Integer> entry : runningTasksByZoneId.entrySet()) {
            tasksByZoneId.put(entry.getKey(), tasksByZoneId.getOrDefault(entry.getKey(), 0) + entry.getValue());
        }

        int taskZoneCounter = tasksByZoneId.getOrDefault(targetZoneId, 0);
        if (taskZoneCounter == 0 || tasksByZoneId.isEmpty()) {
            return 1.0;
        }

        double sum = 0.0;
        for (int i : tasksByZoneId.values()) {
            sum += i;
        }
        double avg = Math.ceil((sum + 1) / Math.max(expectedValues, tasksByZoneId.size()));
        if (taskZoneCounter < avg) {
            return (avg - (double) taskZoneCounter) / avg;
        }
        return 0.0;
    }
}
