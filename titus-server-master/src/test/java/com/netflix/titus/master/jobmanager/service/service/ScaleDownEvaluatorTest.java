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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.FeatureActivationConfiguration;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.common.data.generator.DataGenerator;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.tuple.Pair;
import org.junit.Test;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.common.util.CollectionsExt.first;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static com.netflix.titus.testkit.model.job.JobGenerator.serviceJobs;
import static com.netflix.titus.testkit.model.job.JobGenerator.serviceTasks;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScaleDownEvaluatorTest {

    private DataGenerator<ServiceJobTask> taskDataGenerator = serviceTasks(
            serviceJobs(changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 1_000)).getValue()
    );

    private final FeatureActivationConfiguration configuration = mock(FeatureActivationConfiguration.class);

    private final ServiceJobTask
            task1 = nextTask("zoneA", "agentA1", TaskState.Started),
            task2 = nextTask("zoneA", "agentA1", TaskState.Started),
            task3 = nextTask("zoneA", "agentA2", TaskState.Started),
            task4 = nextTask("zoneB", "agentB1", TaskState.Started),
            task5 = nextTask("zoneB", "agentB1", TaskState.Started),
            task6 = nextTask("zoneB", "agentB1", TaskState.Started),
            task7 = nextTask("zoneB", "agentB1", TaskState.Started),
            task8 = nextTask("zoneB", "agentB1", TaskState.Started),
            task9 = nextTask("zoneC", "agentC1", TaskState.Started),
            task10 = nextTask("zoneA", "agentA1", TaskState.Accepted),
            task11 = nextTask("zoneA", "agentA2", TaskState.KillInitiated),
            task12 = nextTask("zoneB", "agentB1", TaskState.StartInitiated),
            task13 = nextTask("zoneC", "agentC1", TaskState.Launched),
            task14 = nextTask("zoneD", "agentD1", TaskState.Accepted);

    @Test
    public void testTasksInKillInitiatedStateAreSelectedFirst() {
        when(configuration.isServiceJobTaskTerminationFavorBinPackingEnabled()).thenReturn(false);
        List<ServiceJobTask> tasks = asList(
                nextTask("zoneA", "agentA1", TaskState.Accepted),
                nextTask("zoneA", "agentA1", TaskState.KillInitiated),
                nextTask("zoneB", "agentB1", TaskState.Started),
                nextTask("zoneB", "agentB1", TaskState.KillInitiated)
        );
        evaluateAndCheck(tasks, 3, TaskState.KillInitiated);
        evaluateAndCheck(tasks, 2, TaskState.KillInitiated);
    }

    @Test
    public void testTasksInAcceptedStateAreSelectedAfterTasksInKillInitiatedState() {
        when(configuration.isServiceJobTaskTerminationFavorBinPackingEnabled()).thenReturn(false);
        List<ServiceJobTask> tasks = asList(
                nextTask("zoneA", "agentA1", TaskState.Accepted),
                nextTask("zoneA", "agentA1", TaskState.Started),
                nextTask("zoneA", "agentA1", TaskState.KillInitiated),
                nextTask("zoneB", "agentB1", TaskState.Accepted),
                nextTask("zoneB", "agentB1", TaskState.Started),
                nextTask("zoneB", "agentB1", TaskState.KillInitiated)
        );
        evaluateAndCheck(tasks, 3, TaskState.KillInitiated, 2, TaskState.Accepted, 1);
        evaluateAndCheck(tasks, 2, TaskState.KillInitiated, 2, TaskState.Accepted, 2);
    }

    @Test
    public void testTasksInLaunchedOrStartInitiatedStateAreSelectedBeforeTasksInStartedState() {
        when(configuration.isServiceJobTaskTerminationFavorBinPackingEnabled()).thenReturn(false);
        List<ServiceJobTask> tasks = asList(
                nextTask("zoneA", "agentA1", TaskState.Launched),
                nextTask("zoneA", "agentA1", TaskState.Started),
                nextTask("zoneB", "agentB1", TaskState.StartInitiated),
                nextTask("zoneB", "agentB1", TaskState.Started)
        );
        evaluateAndCheck(tasks, 2, TaskState.Launched, 1, TaskState.StartInitiated, 1);
    }

    @Test
    public void testScaleDownToZero() {
        when(configuration.isServiceJobTaskTerminationFavorBinPackingEnabled()).thenReturn(false);
        List<ServiceJobTask> tasks = asList(
                nextTask("zoneA", "agentA1", TaskState.Launched),
                nextTask("zoneA", "agentA1", TaskState.StartInitiated),
                nextTask("zoneB", "agentB1", TaskState.Started),
                nextTask("zoneB", "agentB1", TaskState.KillInitiated)
        );
        List<ServiceJobTask> toRemove = doEvaluate(tasks, 0);
        assertThat(toRemove).hasSize(4);
    }

    @Test
    public void testScaleDownFavorSpreading() {
        when(configuration.isServiceJobTaskTerminationFavorBinPackingEnabled()).thenReturn(false);
        List<Pair<Integer, Set<ServiceJobTask>>> testDesiredRemovedPairs = asList(
                Pair.of(13, asSet(task11)),
                Pair.of(11, asSet(task10, task14)),
                Pair.of(9, asSet(task12, task13)),
                Pair.of(4, asSet(task4, task5, task6, task1, task7)),
                Pair.of(0, asSet(task2, task3, task8, task9))
        );
        List<ServiceJobTask> tasks = new ArrayList<>();
        CollectionsExt.addAll(tasks, new ServiceJobTask[]{task1, task2, task3, task4, task5, task6, task7, task8, task9, task10, task11, task12, task13, task14});
        int i = 0;
        while (!tasks.isEmpty()) {
            Set<ServiceJobTask> expectedToRemove = testDesiredRemovedPairs.get(i).getRight();
            evaluateAndCheckRemoved(tasks, testDesiredRemovedPairs.get(i).getLeft(), expectedToRemove);
            tasks.removeAll(expectedToRemove);
            i++;
        }
    }

    @Test
    public void testScaleDownFavorBinPacking() {
        when(configuration.isServiceJobTaskTerminationFavorBinPackingEnabled()).thenReturn(true);
        List<Pair<Integer, Set<ServiceJobTask>>> testDesiredRemovedPairs = asList(
                Pair.of(13, asSet(task11)),
                Pair.of(11, asSet(task10, task14)),
                Pair.of(9, asSet(task12, task13)),
                Pair.of(4, asSet(task3, task9, task1, task2, task4)),
                Pair.of(0, asSet(task5, task6, task7, task8))
        );
        List<ServiceJobTask> tasks = new ArrayList<>();
        CollectionsExt.addAll(tasks, new ServiceJobTask[]{task1, task2, task3, task4, task5, task6, task7, task8, task9, task10, task11, task12, task13, task14});
        int i = 0;
        while (!tasks.isEmpty()) {
            Set<ServiceJobTask> expectedToRemove = testDesiredRemovedPairs.get(i).getRight();
            evaluateAndCheckRemoved(tasks, testDesiredRemovedPairs.get(i).getLeft(), expectedToRemove);
            tasks.removeAll(expectedToRemove);
            i++;
        }
    }

    @Test
    public void testLargeTaskGroupsAreScaledDownFirst() {
        when(configuration.isServiceJobTaskTerminationFavorBinPackingEnabled()).thenReturn(false);
        List<ServiceJobTask> tasks = asList(
                nextTask("zoneA", "agentA1", TaskState.Launched),
                nextTask("zoneB", "agentB1", TaskState.Launched),
                nextTask("zoneB", "agentB1", TaskState.Launched),
                nextTask("zoneB", "agentB1", TaskState.StartInitiated),
                nextTask("zoneB", "agentB1", TaskState.StartInitiated),
                nextTask("zoneB", "agentB1", TaskState.StartInitiated),
                nextTask("zoneC", "agentC1", TaskState.Launched)
        );
        List<ServiceJobTask> toRemove = doEvaluate(tasks, 3);
        Map<String, List<ServiceJobTask>> toRemoveGrouped = groupByZone(toRemove);
        assertThat(toRemoveGrouped).hasSize(1);
        assertThat(first(toRemoveGrouped.keySet())).isEqualTo("zoneB");
        assertThat(toRemoveGrouped.get("zoneB")).hasSize(4);
    }

    private List<ServiceJobTask> doEvaluate(List<ServiceJobTask> tasks, int expectedSize) {
        List<ServiceJobTask> toRemove = ScaleDownEvaluator.selectTasksToTerminate(configuration, tasks, expectedSize, TitusRuntimes.test());
        checkAreForDuplicates(toRemove);
        return toRemove;
    }

    private void evaluateAndCheck(List<ServiceJobTask> tasks, int expectedSize, TaskState expectedSelectedTasksState) {
        List<ServiceJobTask> toRemove = doEvaluate(tasks, expectedSize);
        assertThat(toRemove).hasSize(tasks.size() - expectedSize);
        toRemove.forEach(t -> assertThat(t.getStatus().getState()).isEqualTo(expectedSelectedTasksState));
    }

    private void evaluateAndCheck(List<ServiceJobTask> tasks, int expectedSize, TaskState expectedState1, int expectedSize1, TaskState expectedState2, int expectedSize2) {
        List<ServiceJobTask> toRemove = doEvaluate(tasks, expectedSize);
        assertThat(toRemove).hasSize(tasks.size() - expectedSize);
        Map<TaskState, List<ServiceJobTask>> byState = toRemove.stream().collect(Collectors.groupingBy(t -> t.getStatus().getState()));
        assertThat(byState.get(expectedState1)).hasSize(expectedSize1);
        assertThat(byState.get(expectedState2)).hasSize(expectedSize2);
    }

    private void checkAreForDuplicates(List<ServiceJobTask> tasks) {
        List<String> duplicatedIds = tasks.stream().collect(Collectors.groupingBy(Task::getId)).values().stream()
                .filter(v -> v.size() > 1).map(l -> l.get(0).getId()).collect(Collectors.toList());
        assertThat(duplicatedIds).isEmpty();
    }

    private Map<String, List<ServiceJobTask>> groupByZone(List<ServiceJobTask> toRemove) {
        return toRemove.stream().collect(Collectors.groupingBy(t -> t.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE)));
    }

    private ServiceJobTask nextTask(String zoneId, String agentId, TaskState taskState) {
        ServiceJobTask task = taskDataGenerator.getValue().toBuilder()
                .withStatus(TaskStatus.newBuilder().withState(taskState).build())
                .addAllToTaskContext(ImmutableMap.of(
                        TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE, zoneId,
                        TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, agentId
                ))
                .build();
        this.taskDataGenerator = taskDataGenerator.apply();
        return task;
    }

    private void evaluateAndCheckRemoved(List<ServiceJobTask> tasks, int desiredSize, Set<ServiceJobTask> removedServiceJobTasks) {
        List<ServiceJobTask> toRemove = doEvaluate(tasks, desiredSize);
        assertThat(toRemove).hasSize(removedServiceJobTasks.size());
        assertThat(toRemove).containsAll(removedServiceJobTasks);
        assertThat(removedServiceJobTasks).containsAll(toRemove);
    }
}