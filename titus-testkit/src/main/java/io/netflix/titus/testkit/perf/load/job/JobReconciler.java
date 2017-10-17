/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.testkit.perf.load.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.netflix.titus.api.endpoint.v2.rest.representation.TaskInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobInfo;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusTaskState;
import io.netflix.titus.testkit.util.TitusV2ModelExt;

import static io.netflix.titus.common.util.CollectionsExt.asSet;

/**
 * These methods may be run concurrently by scenario runner, and all jobs monitor, so we need to synchronize access
 * to this object.
 */
class JobReconciler {

    private static final Set<TitusTaskState> ALIVE_TASKS = asSet(
            TitusTaskState.QUEUED, TitusTaskState.DISPATCHED, TitusTaskState.STARTING, TitusTaskState.RUNNING
    );

    private final int expected;
    private String jobId;
    private boolean firstReconcile = true;
    private Map<String, TaskInfo> knownAliveTasks = Collections.emptyMap();
    private int lastReportedInconsistentSize = -1;
    private Set<String> requestedTaskKills = new HashSet<>();

    JobReconciler(int expected) {
        this.expected = expected;
    }

    synchronized List<JobChangeEvent> jobSubmitted(String jobId) {
        this.jobId = jobId;
        return Collections.singletonList(JobChangeEvent.onJobSubmit(jobId));
    }

    synchronized List<JobChangeEvent> jobKillRequested() {
        return Collections.singletonList(JobChangeEvent.onJobKillRequested(jobId));
    }

    synchronized List<JobChangeEvent> taskKillRequested(String taskId) {
        requestedTaskKills.add(taskId);
        return Collections.singletonList(JobChangeEvent.onTaskKillRequested(jobId, taskId));
    }

    synchronized List<JobChangeEvent> taskTerminateAndShrinkRequested(String taskId) {
        requestedTaskKills.add(taskId);
        return Collections.singletonList(JobChangeEvent.onTaskTerminateAndShrink(jobId, taskId));
    }


    synchronized List<JobChangeEvent> updateInstanceCountRequested(String jobId, int min, int desired, int max) {
        return Collections.singletonList(JobChangeEvent.onUpdateInstanceCountRequest(jobId, min, desired, max));
    }

    synchronized List<JobChangeEvent> jobFinished(TitusJobInfo jobInfo) {
        List<JobChangeEvent> events = new ArrayList<>();
        Map<String, TaskInfo> tasks = TitusV2ModelExt.toMap(jobInfo.getTasks());

        knownAliveTasks.values().forEach(t -> {
            TaskInfo lastKnown = tasks.get(t.getId());
            TitusTaskState finalState = lastKnown == null ? TitusTaskState.UNKNOWN : lastKnown.getState();
            events.add(JobChangeEvent.onTaskStateChange(jobId, t.getId(), finalState));
        });

        events.add(JobChangeEvent.onJobFinished(jobId, jobInfo.getState()));
        return events;
    }

    synchronized List<JobChangeEvent> reconcile(TitusJobInfo jobInfo) {
        List<JobChangeEvent> events = new ArrayList<>();

        if (firstReconcile) {
            knownAliveTasks = findAlive(jobInfo);
            knownAliveTasks.values().forEach(t -> events.add(JobChangeEvent.onTaskCreate(jobInfo.getId(), t)));

        } else {
            Map<String, TaskInfo> tasks = TitusV2ModelExt.toMap(jobInfo.getTasks());
            Map<String, TaskInfo> newAliveTasks = findAlive(jobInfo);

            // Find lost tasks
            Set<String> lostTasks = new HashSet<>(knownAliveTasks.keySet());
            lostTasks.removeAll(newAliveTasks.keySet());
            lostTasks.removeAll(requestedTaskKills);
            lostTasks.forEach(tid -> {
                TaskInfo lastUpdate = tasks.get(tid);
                TitusTaskState finalState = lastUpdate == null ? TitusTaskState.UNKNOWN : lastUpdate.getState();
                events.add(JobChangeEvent.onTaskStateChange(jobId, tid, finalState));
            });

            // Find new tasks or tasks with state changes
            for (TaskInfo newTask : newAliveTasks.values()) {
                TaskInfo knownTask = knownAliveTasks.get(newTask.getId());
                if (knownTask == null) {
                    events.add(JobChangeEvent.onTaskCreate(jobInfo.getId(), newTask));
                } else {
                    if (knownTask.getState() != newTask.getState()) {
                        events.add(JobChangeEvent.onTaskStateChange(jobId, newTask.getId(), newTask.getState()));
                    }
                }
            }
            this.knownAliveTasks = newAliveTasks;
        }

        // Check if there is expected number of tasks
        if (knownAliveTasks.size() == expected) {
            if (!firstReconcile && lastReportedInconsistentSize != -1) {
                lastReportedInconsistentSize = -1;
                events.add(JobChangeEvent.onConsistencyRestore(jobId, expected));
            }
        } else {
            if (lastReportedInconsistentSize != knownAliveTasks.size()) {
                lastReportedInconsistentSize = knownAliveTasks.size();
                events.add(JobChangeEvent.onInconsistentState(jobId, expected, knownAliveTasks.size()));
            }
        }

        firstReconcile = false;

        return events;
    }

    private static Map<String, TaskInfo> findAlive(TitusJobInfo jobInfo) {
        if (jobInfo.getTasks() == null) {
            return Collections.emptyMap();
        }
        Map<String, TaskInfo> result = new HashMap<>();
        jobInfo.getTasks().forEach(t -> {
            if (ALIVE_TASKS.contains(t.getState())) {
                result.put(t.getId(), t);
            }
        });
        return result;
    }
}
