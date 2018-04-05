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

package com.netflix.titus.master.job;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.model.v2.JobCompletedReason;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.master.MetricConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.max;

/**
 * A helper class accompanying {@link V2JobMgrIntf} implementations to track list of temporary disabled agents for a job.
 */
class ExcludedAgentsTracker {

    private static final Logger logger = LoggerFactory.getLogger(ExcludedAgentsTracker.class);

    private static final int MAX_QUEUE_SIZE = 100;
    private static final long MIN_RUNNING_TIME_FOR_DISABLE_MS = 30_000;

    private final String jobId;
    private final JobManagerConfiguration configuration;

    private final ConcurrentMap<String, Long> excludedAgentIds = new ConcurrentHashMap<>();
    private final BlockingQueue<Pair<String, Long>> disableTimes = new PriorityBlockingQueue<>(MAX_QUEUE_SIZE, Comparator.comparingLong(Pair::getRight));

    ExcludedAgentsTracker(String jobId, String appName, JobManagerConfiguration configuration, Registry registry) {
        this.jobId = jobId;
        this.configuration = configuration;
        registry.gauge(
                registry.createId(MetricConstants.METRIC_SCHEDULING_JOB + "excludedAgentsTracker.queue", "t.jobId", jobId, "t.application", appName),
                this, self -> self.excludedAgentIds.size()
        );
    }

    void update(V2WorkerMetadata task, V2JobState newState, JobCompletedReason newReason) {
        String agentHost = task.getSlave();
        if (agentHost == null) {
            return;
        }

        if (newState == V2JobState.Completed) {
            if (excludedAgentIds.remove(agentHost) != null) {
                logger.debug("Removed agent {} from disable list for job {}", agentHost, jobId);
            }
        } else if (newState == V2JobState.Failed && shouldExclude(newReason)) {
            // Task may finish before moving between all these states
            long startTime = max(task.getLaunchedAt(), max(task.getStartingAt(), task.getStartedAt()));

            if (configuration.getAgentDisableTimeMs() > 0 && startTime > 0) {

                long now = System.currentTimeMillis();
                long completedTime = task.getCompletedAt() > 0 ? task.getCompletedAt() : now;
                long taskRunningTime = completedTime - startTime;

                if (taskRunningTime < MIN_RUNNING_TIME_FOR_DISABLE_MS) {
                    Long disableEndTime = now + configuration.getAgentDisableTimeMs();
                    excludedAgentIds.put(agentHost, disableEndTime);
                    disableTimes.add(Pair.of(agentHost, disableEndTime));

                    logger.debug("Added agent {} to disable list for job {} (total={})", agentHost, jobId, excludedAgentIds.size());
                }
            }
        }
    }

    void finish() {
        excludedAgentIds.clear();
        disableTimes.clear();
    }

    Set<String> getExcludedAgents() {
        if (configuration.isEnableAgentTracking()) {
            trimAndRemoveExpiredEntries();
            return excludedAgentIds.keySet();
        } else {
            return Collections.emptySet();
        }
    }

    private boolean shouldExclude(JobCompletedReason reason) {
        return reason == JobCompletedReason.Error || reason == JobCompletedReason.Failed || reason == JobCompletedReason.Lost;
    }

    private void trimAndRemoveExpiredEntries() {
        int tooManyItems = disableTimes.size() - MAX_QUEUE_SIZE;
        for (int i = 0; i < tooManyItems; i++) {
            removeFirstDisableEntry();
        }

        long now = System.currentTimeMillis();
        Pair<String, Long> next;
        while ((next = disableTimes.peek()) != null && next.getRight() <= now) {
            removeFirstDisableEntry();
        }
    }

    private void removeFirstDisableEntry() {
        Pair<String, Long> entry = disableTimes.poll();
        if (entry != null) {
            excludedAgentIds.remove(entry.getLeft(), entry.getRight());
        }
    }
}
