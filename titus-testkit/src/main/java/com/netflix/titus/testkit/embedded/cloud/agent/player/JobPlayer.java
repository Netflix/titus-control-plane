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

package com.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import rx.Scheduler;

class JobPlayer {

    private static final long EXPIRY_TIME_MS = 600_000;

    private final List<Pair<RuleSelector, ContainerRules>> rules;
    private final Scheduler scheduler;

    private final ConcurrentMap<String, ContainerPlayer> players = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> taskIndexer = new ConcurrentHashMap<>();
    private final AtomicInteger nextIndex = new AtomicInteger();

    private long lastUpdateTimestamp;

    JobPlayer(List<Pair<RuleSelector, ContainerRules>> rules, Scheduler scheduler) {
        this.rules = rules;
        this.scheduler = scheduler;
        this.lastUpdateTimestamp = scheduler.now();
    }

    void shutdown() {
        players.values().forEach(ContainerPlayer::shutdown);
    }

    boolean isStale() {
        players.values().removeIf(ContainerPlayer::isTerminated);
        if (players.isEmpty()) {
            long lastUpdateMs = scheduler.now() - lastUpdateTimestamp;
            return lastUpdateMs > EXPIRY_TIME_MS;
        }
        return false;
    }

    void play(TaskExecutorHolder taskHolder) {
        int index = findIndexOf(taskHolder);
        int resubmit = findResubmitOf(taskHolder);
        for (Pair<RuleSelector, ContainerRules> rule : rules) {
            if (matches(rule.getLeft(), index, resubmit, taskHolder.getAgent().getId())) {
                players.put(taskHolder.getTaskId(), new ContainerPlayer(taskHolder, rule.getRight(), scheduler));
            }
        }
    }

    private boolean matches(RuleSelector selector, int taskIndex, int taskResubmit, String agentId) {
        return selector.getSlots().matches(taskIndex)
                && selector.getResubmits().matches(taskResubmit)
                && (selector.getInstanceIds().isEmpty() || selector.getInstanceIds().contains(agentId));
    }

    private int findIndexOf(TaskExecutorHolder taskHolder) {
        String taskIndex = taskHolder.getEnv().get(TaskAttributes.TASK_ATTRIBUTES_TASK_INDEX);
        if (taskIndex != null) {
            return Integer.parseInt(taskIndex);
        }
        String originalId = taskHolder.getEnv().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_TASK_ORIGINAL_ID, taskHolder.getTaskId());
        return taskIndexer.computeIfAbsent(originalId, id -> nextIndex.getAndIncrement());
    }

    private int findResubmitOf(TaskExecutorHolder taskHolder) {
        return Integer.parseInt(taskHolder.getEnv().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_RESUBMIT_NUMBER, "0"));
    }
}
