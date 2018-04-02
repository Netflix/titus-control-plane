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

package com.netflix.titus.master.jobmanager.service.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;

import static com.netflix.titus.common.util.code.CodeInvariants.codeInvariants;

/**
 * Helper class for evaluating which tasks from a job active task set should be terminated during scale down process.
 * Terminating tasks in random order might result in an undesired task setup, like having all tasks running on one agent
 * instance (availability issue) or in a single zone.
 * <p>
 * As Titus does not support scale down polices that would allow a user to define explicitly the termination rules, we
 * provide here a set of reasonable rules, that should give much better behavior than blind task termination.
 * <p>
 * <h1>Termination steps</h1>
 * An active task can be in one of the following states: Active, Launched, StartInitiated, Started and KillInitiated.
 * To maximize system availability, we consider for termination tasks in Started state only if there are no tasks in other
 * states.
 * <p>
 * <h2>Step 1: scale down task in KillInitiated state</h2>
 * As tasks in this state are in the termination process already, scaling them down means they should not be restarted after
 * they move to Finished state.
 * <p>
 * <h2>Step 2: scale down tasks in Accepted state</h2>
 * These task have no resources allocated yet, and it is not guaranteed that they can be successfully started.
 * Tasks in Accepted state are not yet associated with any agent, so no additional rules must be applied here.
 * <p>
 * <h2>Step 3: scale down tasks in Launched and StartInitiated states</h2>
 * These states can be considered as equivalent. The group of tasks with these states should be evaluated according
 * to rules defined in 'Equivalent group termination rules' in section below.
 * <p>
 * <h2>Step 4: scale down tasks in Started state</h2>
 * As in step 3, this task set should be evaluated according to rules defined in 'Equivalent group termination rules'.
 * <p>
 * <h1>Equivalent group termination rules</h1>
 * <p>
 * The selection process described below is applied repeatedly until the desired number of tasks is terminated. At each
 * step a subset of tasks is selected. The last step ('Terminate oldest task first'), makes final selection.
 * <p>
 * <h2>Scale down largest groups of tasks on an agent</h2>
 * To maximize availability we should eliminate large groups of tasks running on the same agent.
 * <p>
 * <h2>Enforce zone balancing</h2>
 * We want our tasks to be zone balanced (if applicable), If one zone runs more tasks than the other, we should terminate
 * tasks in this zone first.
 * <p>
 * <h2>Terminate oldest task first</h2>
 * From the given collection of tasks, select the task with the oldest creation timestamp and terminate it.
 * If there are more tasks to terminate, restart the evaluation process.
 */
class ScaleDownEvaluator {

    static List<ServiceJobTask> selectTasksToTerminate(List<ServiceJobTask> allTasks, int expectedSize) {
        int targetTerminateCount = allTasks.size() - expectedSize;
        if (targetTerminateCount <= 0) {
            return Collections.emptyList();
        }

        List<ServiceJobTask> tasksToKill = new ArrayList<>();

        // Step 1: scale down task in KillInitiated state
        Pair<List<ServiceJobTask>, List<ServiceJobTask>> killInitiatedAndRunnableGroups = splitIntoKillInitiatedAndRunnableGroups(allTasks);
        if (appendCandidatesToTerminate(tasksToKill, killInitiatedAndRunnableGroups.getLeft(), targetTerminateCount)) {
            return tasksToKill;
        }
        List<ServiceJobTask> runnableTasks = killInitiatedAndRunnableGroups.getRight();

        // Step 2: scale down tasks in Accepted state
        Pair<List<ServiceJobTask>, List<ServiceJobTask>> acceptedAndPlacedOnAgentsGroup = splitIntoAcceptedAndPlacedOnAgentsGroup(runnableTasks);
        if (appendCandidatesToTerminate(tasksToKill, acceptedAndPlacedOnAgentsGroup.getLeft(), targetTerminateCount)) {
            return tasksToKill;
        }
        List<ServiceJobTask> tasksOnAgent = acceptedAndPlacedOnAgentsGroup.getRight();

        // Step 3: scale down tasks in Launched and StartInitiated states
        Pair<List<ServiceJobTask>, List<ServiceJobTask>> notStartedAndStartedTaskGroups = splitIntoNotStartedAndStartedTaskGroups(tasksOnAgent);
        List<ServiceJobTask> notStartedToRemove = selectTasksToTerminateInEquivalenceGroup(notStartedAndStartedTaskGroups.getLeft(), targetTerminateCount - tasksToKill.size());
        if (appendCandidatesToTerminate(tasksToKill, notStartedToRemove, targetTerminateCount)) {
            return tasksToKill;
        }
        List<ServiceJobTask> startedTasks = notStartedAndStartedTaskGroups.getRight();

        // Step 4: scale down tasks in Started state
        List<ServiceJobTask> startedToRemove = selectTasksToTerminateInEquivalenceGroup(startedTasks, targetTerminateCount - tasksToKill.size());
        appendCandidatesToTerminate(tasksToKill, startedToRemove, targetTerminateCount);

        // Extra check in case we messed up somewhere.
        if (tasksToKill.size() != targetTerminateCount) {
            codeInvariants().inconsistent("Wrong number of tasks to terminate %s (expected) != %s (actual). Got list: %s", targetTerminateCount, tasksToKill.size(), tasksToKill);
            if (tasksToKill.size() > targetTerminateCount) {
                tasksToKill = tasksToKill.subList(0, targetTerminateCount);
            }
        }

        return tasksToKill;
    }

    /**
     * Add candidate tasks to the provided accumulator, up to the request number of tasks to terminate.
     *
     * @return true if the accumulator (tasks to terminate list) reached its maximum size, false otherwise
     */
    private static boolean appendCandidatesToTerminate(List<ServiceJobTask> accumulator, List<ServiceJobTask> candidates, int targetTerminateCount) {
        if (accumulator.size() + candidates.size() >= targetTerminateCount) {
            accumulator.addAll(candidates.subList(0, targetTerminateCount - accumulator.size()));
            return true;
        }
        accumulator.addAll(candidates);
        return false;
    }

    private static Pair<List<ServiceJobTask>, List<ServiceJobTask>> splitIntoKillInitiatedAndRunnableGroups(List<ServiceJobTask> tasks) {
        return CollectionsExt.split(tasks, t -> t.getStatus().getState() == TaskState.KillInitiated);
    }

    private static Pair<List<ServiceJobTask>, List<ServiceJobTask>> splitIntoAcceptedAndPlacedOnAgentsGroup(List<ServiceJobTask> tasks) {
        return CollectionsExt.split(tasks, t -> t.getStatus().getState() == TaskState.Accepted);
    }

    private static Pair<List<ServiceJobTask>, List<ServiceJobTask>> splitIntoNotStartedAndStartedTaskGroups(List<ServiceJobTask> tasks) {
        return CollectionsExt.split(tasks, t -> t.getStatus().getState() != TaskState.Started);
    }

    private static List<ServiceJobTask> selectTasksToTerminateInEquivalenceGroup(List<ServiceJobTask> allTasks, int targetTerminateCount) {
        List<ServiceJobTask> tasksToKill = new ArrayList<>();
        Region region = new Region(allTasks);

        while (tasksToKill.size() < targetTerminateCount && region.hasMoreTasks()) {
            // Step 1: select largest groups of tasks to scale down
            int largestGroup = region.getLargestTaskGroup();

            // Step 2: kill tasks from the largest groups, trying to maintain zone balancing
            boolean hasMore = true;
            while (tasksToKill.size() < targetTerminateCount && hasMore) {
                Optional<Zone> zoneOpt = region.getLargestZoneWithTaskGroupSize(largestGroup);
                if (hasMore = zoneOpt.isPresent()) {
                    // Step 3: remove oldest task
                    Optional<ServiceJobTask> removedTask = zoneOpt.get().removeOldestTaskFromLargestTaskGroup();
                    removedTask.ifPresent(tasksToKill::add);
                    if (!removedTask.isPresent()) {
                        codeInvariants().inconsistent("Expected task, but found nothing. Terminating evaluation loop of job %s", allTasks.get(0).getJobId());
                        return tasksToKill;
                    }
                }
            }
        }

        return tasksToKill;
    }

    static class Region {

        private final Map<String, Zone> zones;

        Region(List<ServiceJobTask> allTasks) {
            Map<String, List<ServiceJobTask>> byZone = new HashMap<>();
            allTasks.forEach(task -> byZone.computeIfAbsent(toZoneId(task), t -> new ArrayList<>()).add(task));
            this.zones = byZone.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new Zone(e.getValue())));
        }

        String toZoneId(Task task) {
            return task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE, "default");
        }

        int getLargestTaskGroup() {
            return zones.values().stream().mapToInt(Zone::getLargestTaskGroupSize).max().orElse(0);
        }

        Optional<Zone> getLargestZoneWithTaskGroupSize(int largestGroup) {
            Zone selectedZone = null;
            for (Zone zone : zones.values()) {
                if (zone.getLargestTaskGroupSize() >= largestGroup) {
                    if (selectedZone == null) {
                        selectedZone = zone;
                    } else {
                        if (selectedZone.getTaskCount() < zone.getTaskCount()) {
                            selectedZone = zone;
                        }
                    }
                }
            }
            return Optional.ofNullable(selectedZone);
        }

        boolean hasMoreTasks() {
            return zones.values().stream().anyMatch(z -> z.getTaskCount() > 0);
        }
    }

    static class Zone {

        private final Map<String, List<ServiceJobTask>> tasksByAgentId;
        private int taskCount;

        Zone(List<ServiceJobTask> tasks) {
            this.taskCount = tasks.size();
            this.tasksByAgentId = new HashMap<>();
            tasks.forEach(task -> tasksByAgentId.computeIfAbsent(toAgentId(task), t -> new ArrayList<>()).add(task));
        }

        String toAgentId(Task task) {
            return task.getTaskContext().getOrDefault(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, "default");
        }

        int getLargestTaskGroupSize() {
            return tasksByAgentId.values().stream().mapToInt(List::size).max().orElse(0);
        }

        Optional<ServiceJobTask> removeOldestTaskFromLargestTaskGroup() {
            if (taskCount == 0) {
                return Optional.empty();
            }
            return tasksByAgentId.entrySet().stream()
                    .max(Comparator.comparingInt(l -> l.getValue().size()))
                    .map(largestGroupEntry -> {
                                List<ServiceJobTask> tasks = largestGroupEntry.getValue();

                                int bestIdx = 0;
                                long bestTimestamp = tasks.get(0).getStatus().getTimestamp();

                                for (int i = 1; i < tasks.size(); i++) {
                                    long currentTimestamp = tasks.get(i).getStatus().getTimestamp();
                                    if (currentTimestamp < bestTimestamp) {
                                        bestIdx = i;
                                        bestTimestamp = currentTimestamp;
                                    }
                                }
                                taskCount--;
                                return tasks.remove(bestIdx);
                            }
                    );
        }

        int getTaskCount() {
            return taskCount;
        }
    }
}
