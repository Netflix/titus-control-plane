package io.netflix.titus.testkit.embedded.cloud.agent.player;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.api.jobmanager.TaskAttributes;
import io.netflix.titus.testkit.embedded.cloud.agent.TaskExecutorHolder;
import rx.Scheduler;

class JobPlayer {

    private static final long EXPIRY_TIME_MS = 600_000;

    private final List<Pair<ContainerSelector, ContainerRules>> rules;
    private final Scheduler scheduler;

    private final ConcurrentMap<String, ContainerPlayer> players = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> taskIndexer = new ConcurrentHashMap<>();
    private final AtomicInteger nextIndex = new AtomicInteger();

    private long lastUpdateTimestamp;

    JobPlayer(List<Pair<ContainerSelector, ContainerRules>> rules, Scheduler scheduler) {
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
        for (Pair<ContainerSelector, ContainerRules> rule : rules) {
            if (matches(rule.getLeft(), index, resubmit)) {
                players.put(taskHolder.getTaskId(), new ContainerPlayer(taskHolder, rule.getRight(), scheduler));
            }
        }
    }

    private boolean matches(ContainerSelector selector, int taskIndex, int taskResubmit) {
        return selector.getSlots().matches(taskIndex) && selector.getResubmits().matches(taskResubmit);
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
