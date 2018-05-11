package com.netflix.titus.master.scheduler.constraint;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.tuple.Pair;

/**
 * Helper class that aggregates task data by multiple criteria used by Fenzo constraint/fitness evaluators.
 */
@Singleton
public class TaskCache {

    private final V3JobOperations v3JobOperations;
    private final AtomicReference<TaskCacheValue> currentCacheValue;

    @Inject
    public TaskCache(V3JobOperations v3JobOperations) {
        this.v3JobOperations = v3JobOperations;
        this.currentCacheValue = new AtomicReference<>();
    }

    public void prepare() {
        currentCacheValue.set(new TaskCacheValue());
    }

    public Map<String, Integer> getTasksByZoneIdCounters(String jobId) {
        return currentCacheValue.get().getTasksByZoneIdCounters(jobId);
    }

    private class TaskCacheValue {

        private final Map<String, Map<String, Integer>> zoneBalanceCountersByJobId;

        private TaskCacheValue() {
            List<Pair<Job, List<Task>>> jobsAndTasks = v3JobOperations.getJobsAndTasks();
            this.zoneBalanceCountersByJobId = buildZoneBalanceCountersByJobId(jobsAndTasks);
        }

        private Map<String, Integer> getTasksByZoneIdCounters(String jobId) {
            return zoneBalanceCountersByJobId.getOrDefault(jobId, Collections.emptyMap());
        }

        private Map<String, Map<String, Integer>> buildZoneBalanceCountersByJobId(List<Pair<Job, List<Task>>> jobsAndTasks) {
            Map<String, Map<String, Integer>> result = new HashMap<>();
            for (Pair<Job, List<Task>> jobAndTask : jobsAndTasks) {
                Map<String, Integer> jobZoneBalancing = new HashMap<>();
                for (Task task : jobAndTask.getRight()) {
                    String zoneId = getZoneId(task);
                    if (zoneId != null) {
                        jobZoneBalancing.put(zoneId, jobZoneBalancing.getOrDefault(zoneId, 0) + 1);
                    }
                }
                result.put(jobAndTask.getLeft().getId(), jobZoneBalancing);
            }
            return result;
        }

        private String getZoneId(Task task) {
            return task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE);
        }
    }
}