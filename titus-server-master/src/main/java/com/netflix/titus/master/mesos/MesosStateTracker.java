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

package com.netflix.titus.master.mesos;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import com.netflix.spectator.api.Gauge;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.config.MasterConfiguration;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks all running containers, by observing task status update requests. We depend here on the
 * reconciliation task periodically requesting full Mesos state. To avoid stale data in local cache,
 * known tasks are kept for two reconciler cycles (as we expect to refresh their state at each cycle), and
 * the unknown tasks are kept for four cycles, as every other cycle only requests status of the all known tasks.
 */
class MesosStateTracker {

    private static final Logger logger = LoggerFactory.getLogger(MesosStateTracker.class);

    private static final String TASKS_METRIC_ID = MetricConstants.METRIC_MESOS + "tasks";
    private static final String TRACKED_TASKS_METRIC_ID = MetricConstants.METRIC_MESOS + "trackedTasks";

    private final MasterConfiguration configuration;

    private final ConcurrentMap<String, TaskTracker> taskTrackers = new ConcurrentHashMap<>();
    private final Queue<TaskTracker> expiryQueue = new ConcurrentLinkedQueue<>();
    private final Clock clock;
    private final Registry registry;

    MesosStateTracker(MasterConfiguration configuration, TitusRuntime titusRuntime) {
        this.configuration = configuration;
        this.clock = titusRuntime.getClock();
        this.registry = titusRuntime.getRegistry();
        PolledMeter.using(registry).withId(registry.createId(TRACKED_TASKS_METRIC_ID)).monitorSize(taskTrackers);
    }

    void knownTaskStatusUpdate(Protos.TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();

        if (isFinal(taskStatus)) {
            removeTask(taskId);
        } else {
            TaskTracker current = taskTrackers.get(taskId);
            TaskTracker effective;
            if (current == null) {
                taskTrackers.put(taskId, effective = new TaskTracker(taskId, true));
            } else {
                current.runningAndKnown();
                effective = current;
            }
            expiryQueue.add(effective);
        }
        processExpiryQueue();
    }

    void unknownTaskStatusUpdate(Protos.TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();

        if (isFinal(taskStatus)) {
            removeTask(taskId);
        } else {
            TaskTracker current = taskTrackers.get(taskId);
            TaskTracker effective;
            if (current == null) {
                taskTrackers.put(taskId, effective = new TaskTracker(taskId, false));
            } else {
                current.runningAndUnknown();
                effective = current;
            }
            expiryQueue.add(effective);
        }

        processExpiryQueue();
    }

    private boolean isFinal(Protos.TaskStatus taskStatus) {
        switch (taskStatus.getState()) {
            case TASK_FINISHED:
            case TASK_FAILED:
            case TASK_KILLED:
            case TASK_ERROR:
            case TASK_LOST:
            case TASK_DROPPED:
                return true;
            case TASK_STAGING:
            case TASK_STARTING:
            case TASK_RUNNING:
            case TASK_KILLING:
            case TASK_UNREACHABLE:
            case TASK_UNKNOWN:
            default:
                return false;
        }
    }

    private void removeTask(String taskId) {
        TaskTracker taskTracker = taskTrackers.remove(taskId);
        if (taskTracker != null) {
            taskTracker.clear();
        }
    }

    private void processExpiryQueue() {
        TaskTracker next;
        while ((next = expiryQueue.peek()) != null && next.isExpired()) {
            next.clear();
            removeTask(next.taskId);

            TaskTracker removed = expiryQueue.remove();
            // We assume this code may run concurrently, hence extra check.
            if (removed != null && removed != next && !removed.isExpired() && taskTrackers.containsKey(removed.taskId)) {
                expiryQueue.add(removed);
            }
        }
    }

    private class TaskTracker {

        private final String taskId;
        private volatile boolean known;
        private volatile long lastUpdateTimestamp;
        private volatile Gauge taskMetric;

        private TaskTracker(String taskId, boolean known) {
            this.taskId = taskId;
            this.known = known;
            this.lastUpdateTimestamp = clock.wallTime();
            newGauge();
        }

        private void runningAndKnown() {
            if (!known) {
                known = true;
                newGauge();
            }
            taskMetric.set(1);
            lastUpdateTimestamp = clock.wallTime();
        }

        private void runningAndUnknown() {
            if (known) {
                known = false;
                newGauge();
            }
            taskMetric.set(1);
            lastUpdateTimestamp = clock.wallTime();
        }

        private void newGauge() {
            logger.debug("Creating new gauge for: taskId={}, known={}", taskId, known);
            clear();
            taskMetric = registry.gauge(TASKS_METRIC_ID,
                    "taskId", taskId,
                    "known", Boolean.toString(known)
            );
            taskMetric.set(1);
        }

        private void clear() {
            if (taskMetric != null) {
                logger.debug("Clearing gauge for: taskId={}, known={}", taskId, known);
                taskMetric.set(0);
            }
        }

        private boolean isExpired() {
            int multiplier = known ? 2 : 4;
            long timeoutMs = multiplier * configuration.getMesosTaskReconciliationIntervalSecs() * 1000L;
            return clock.wallTime() > (lastUpdateTimestamp + timeoutMs);
        }
    }
}
