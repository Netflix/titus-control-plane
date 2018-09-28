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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.patterns.PolledMeter;
import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.config.MasterConfiguration;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Scheduler;
import rx.Subscription;

/**
 * Tracks all running containers, by observing task status update requests. We depend here on the
 * reconciliation task periodically requesting full Mesos state. To avoid stale data in local cache,
 * known tasks are kept for two reconciler cycles (as we expect to refresh their state at each cycle), and
 * the unknown tasks are kept for four cycles, as every other cycle only requests status of the all known tasks.
 */
class MesosStateTracker {

    private static final Logger logger = LoggerFactory.getLogger(MesosStateTracker.class);

    private static final String TASKS_METRIC_ID = MetricConstants.METRIC_MESOS + "tasks";

    /**
     * Amount of time that must be passed for a task in the unknown state, to be reported as stable.
     */
    private static final long INTERVAL_THRESHOLD_MS = 10 * 60_000;

    private static final long REPORTING_INTERVAL_MS = 60_000;

    private final Cache<String, Protos.TaskStatus> knownTasks;
    private final Cache<String, LostTask> unknownTasks;
    private final TitusRuntime titusRuntime;

    private final Subscription reporterSubscription;

    MesosStateTracker(MasterConfiguration configuration, TitusRuntime titusRuntime, Scheduler scheduler) {

        this.knownTasks = CacheBuilder.newBuilder()
                .expireAfterWrite(configuration.getMesosTaskReconciliationIntervalSecs() * 2, TimeUnit.SECONDS)
                .build();
        this.unknownTasks = CacheBuilder.newBuilder()
                .expireAfterWrite(configuration.getMesosTaskReconciliationIntervalSecs() * 4, TimeUnit.SECONDS)
                .build();
        this.titusRuntime = titusRuntime;

        Registry registry = titusRuntime.getRegistry();
        PolledMeter.using(registry).withId(registry.createId(TASKS_METRIC_ID, "known", "true")).monitorValue(this, self -> self.knownTasks.size());
        PolledMeter.using(registry).withId(registry.createId(TASKS_METRIC_ID, "known", "false", "onlyStable", "false")).monitorValue(this, self -> self.unknownTasks.size());
        PolledMeter.using(registry).withId(registry.createId(TASKS_METRIC_ID, "known", "false", "onlyStable", "true")).monitorValue(this, self -> countStableUnknown());

        this.reporterSubscription = ObservableExt.schedule(
                "titus.mesos.stateTracker",
                registry,
                "mesosStateTrackerReporter",
                Completable.fromAction(this::doReport),
                REPORTING_INTERVAL_MS,
                REPORTING_INTERVAL_MS,
                TimeUnit.MILLISECONDS,
                scheduler
        ).subscribe();
    }

    void shutdown() {
        ObservableExt.safeUnsubscribe(reporterSubscription);
    }

    void knownTaskStatusUpdate(Protos.TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();
        knownTasks.put(taskId, taskStatus);
        unknownTasks.invalidate(taskId);

    }

    void unknownTaskStatusUpdate(Protos.TaskStatus taskStatus) {
        String taskId = taskStatus.getTaskId().getValue();
        knownTasks.invalidate(taskId);
        LostTask previous = unknownTasks.getIfPresent(taskId);
        if (previous == null) {
            unknownTasks.put(taskId, new LostTask(taskId, taskStatus, titusRuntime.getClock().wallTime(), false));
        } else {
            unknownTasks.put(taskId, previous.updateState(taskStatus));
        }
    }

    private int countStableUnknown() {
        List<LostTask> allUnknown = new ArrayList<>(unknownTasks.asMap().values());
        long now = System.currentTimeMillis();
        int count = 0;
        for (LostTask lostTask : allUnknown) {
            if (lostTask.isStable(now)) {
                count++;
            }
        }
        return count;
    }

    private void doReport() {
        try {
            List<LostTask> allUnknown = new ArrayList<>(unknownTasks.asMap().values());
            long now = titusRuntime.getClock().wallTime();
            for (LostTask lostTask : allUnknown) {
                if (lostTask.isStable(now) && !lostTask.isReported()) {
                    titusRuntime.getSystemLogService().submit(lostTask.toEvent());
                    unknownTasks.put(lostTask.getTaskId(), lostTask.markAsReportedInSysLog());
                }
            }
        } catch (Exception e) {
            logger.warn("Unexpected error in the reporting loop", e);
        }
    }

    private class LostTask {

        private final String taskId;
        private final Protos.TaskStatus taskStatus;
        private final long timestamp;
        private final boolean reported;

        private LostTask(String taskId, Protos.TaskStatus taskStatus, long timestamp, boolean reported) {
            this.taskId = taskId;
            this.taskStatus = taskStatus;
            this.timestamp = timestamp;
            this.reported = reported;
        }

        public String getTaskId() {
            return taskId;
        }

        private boolean isReported() {
            return reported;
        }

        private LostTask updateState(Protos.TaskStatus taskStatus) {
            return new LostTask(taskId, taskStatus, timestamp, reported);
        }

        private LostTask markAsReportedInSysLog() {
            return new LostTask(taskId, taskStatus, timestamp, true);
        }

        private boolean isStable(long now) {
            return timestamp + INTERVAL_THRESHOLD_MS < now;
        }

        private SystemLogEvent toEvent() {
            return SystemLogEvent.newBuilder()
                    .withComponent("mesos")
                    .withCategory(SystemLogEvent.Category.Permanent)
                    .withPriority(SystemLogEvent.Priority.Info)
                    .withMessage("Found orphaned task in Mesos: taskId=" + taskId)
                    .withContext(Collections.singletonMap("taskState", "" + taskStatus.getState()))
                    .build();
        }
    }
}
