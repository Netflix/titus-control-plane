package io.netflix.titus.master.jobmanager.service.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import io.netflix.titus.api.jobmanager.model.job.ServiceJobTask;
import io.netflix.titus.api.jobmanager.model.job.Task;
import io.netflix.titus.api.jobmanager.model.job.TaskState;
import io.netflix.titus.api.jobmanager.model.job.TaskStatus;
import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.api.jobmanager.TaskAttributes;
import org.junit.Test;

import static io.netflix.titus.api.jobmanager.model.job.JobFunctions.changeServiceJobCapacity;
import static io.netflix.titus.common.util.CollectionsExt.first;
import static io.netflix.titus.testkit.model.job.JobDescriptorGenerator.oneTaskServiceJobDescriptor;
import static io.netflix.titus.testkit.model.job.JobGenerator.serviceJobs;
import static io.netflix.titus.testkit.model.job.JobGenerator.serviceTasks;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ScaleDownEvaluatorTest {

    private DataGenerator<ServiceJobTask> taskDataGenerator = serviceTasks(
            serviceJobs(changeServiceJobCapacity(oneTaskServiceJobDescriptor(), 1_000)).getValue()
    );

    @Test
    public void testTasksInKillInitiatedStateAreSelectedFirst() {
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
    public void testLargeTaskGroupsAreScaledDownFirst() {
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
        List<ServiceJobTask> toRemove = ScaleDownEvaluator.selectTasksToTerminate(tasks, expectedSize);
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
                .withTaskContext(ImmutableMap.of(
                        TaskAttributes.TASK_ATTRIBUTES_AGENT_ZONE, zoneId,
                        TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, agentId
                ))
                .build();
        this.taskDataGenerator = taskDataGenerator.apply();
        return task;
    }
}