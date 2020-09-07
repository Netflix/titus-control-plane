package com.netflix.titus.supplementary.relocation.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.model.ExecutionId;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.runtime.connector.jobmanager.JobDataReplicator;
import com.netflix.titus.runtime.connector.jobmanager.JobManagementClient;
import com.netflix.titus.supplementary.relocation.RelocationConfiguration;
import com.netflix.titus.supplementary.relocation.connector.Node;
import com.netflix.titus.supplementary.relocation.connector.NodeDataResolver;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultNodeConditionControllerTest {

    enum NodeIds {
        node1,
        node2,
        node3
    }

    @Test
    public void noTerminationsOnDataStaleness() {
        TitusRuntime titusRuntime = mock(TitusRuntime.class);
        when(titusRuntime.getRegistry()).thenReturn(new DefaultRegistry());

        RelocationConfiguration configuration = mock(RelocationConfiguration.class);
        when(configuration.getBadNodeConditionPattern()).thenReturn(".*Problem");
        when(configuration.isTaskTerminationOnBadNodeConditionEnabled()).thenReturn(true);
        when(configuration.getDataStalenessThresholdMs()).thenReturn(8000L);

        NodeDataResolver nodeDataResolver = mock(NodeDataResolver.class);
        when(nodeDataResolver.getStalenessMs()).thenReturn(5L);

        JobDataReplicator jobDataReplicator = mock(JobDataReplicator.class);
        when(jobDataReplicator.getStalenessMs()).thenReturn(10L);

        ReadOnlyJobOperations readOnlyJobOperations = mock(ReadOnlyJobOperations.class);

        JobManagementClient jobManagementClient = mock(JobManagementClient.class);
        Set<String> terminatedTaskIds = new HashSet<>();
        when(jobManagementClient.killTask(anyString(), anyBoolean(), any())).thenAnswer(invocation -> {
            String taskIdToBeTerminated = invocation.getArgument(0);
            terminatedTaskIds.add(taskIdToBeTerminated);
            return Mono.empty();
        });

        DefaultNodeConditionController nodeConditionCtrl = new DefaultNodeConditionController(configuration, nodeDataResolver, jobDataReplicator,
                readOnlyJobOperations, jobManagementClient, titusRuntime);

        ExecutionContext executionContext = ExecutionContext.newBuilder().withIteration(ExecutionId.initial()).build();
        StepVerifier.create(nodeConditionCtrl.handleNodesWithBadCondition(executionContext))
                .verifyComplete();

        // No tasks terminated
        assertThat(terminatedTaskIds).isEmpty();
    }


    @Test
    public void checkTasksTerminatedDueToBadNodeConditions() {
        // Mock jobs, tasks & nodes
        Map<String, Node> nodeMap = buildNodes();
        List<Job<BatchJobExt>> jobs = getJobs();
        Map<String, List<Task>> tasksByJobIdMap = buildTasksForJobAndNodeAssignment(new ArrayList<>(nodeMap.values()), jobs);


        TitusRuntime titusRuntime = mock(TitusRuntime.class);
        when(titusRuntime.getRegistry()).thenReturn(new DefaultRegistry());

        RelocationConfiguration configuration = mock(RelocationConfiguration.class);
        when(configuration.getBadNodeConditionPattern()).thenReturn(".*Failure");
        when(configuration.isTaskTerminationOnBadNodeConditionEnabled()).thenReturn(true);

        NodeDataResolver nodeDataResolver = mock(NodeDataResolver.class);
        when(nodeDataResolver.resolve()).thenReturn(nodeMap);

        JobDataReplicator jobDataReplicator = mock(JobDataReplicator.class);
        when(jobDataReplicator.getStalenessMs()).thenReturn(0L);

        ReadOnlyJobOperations readOnlyJobOperations = mock(ReadOnlyJobOperations.class);
        when(readOnlyJobOperations.getJobs()).thenReturn(new ArrayList<>(jobs));
        tasksByJobIdMap.forEach((key, value) -> when(readOnlyJobOperations.getTasks(key)).thenReturn(value));

        JobManagementClient jobManagementClient = mock(JobManagementClient.class);
        Set<String> terminatedTaskIds = new HashSet<>();
        when(jobManagementClient.killTask(anyString(), anyBoolean(), any())).thenAnswer(invocation -> {
            String taskIdToBeTerminated = invocation.getArgument(0);
            terminatedTaskIds.add(taskIdToBeTerminated);
            return Mono.empty();
        });

        DefaultNodeConditionController nodeConditionCtrl = new DefaultNodeConditionController(configuration, nodeDataResolver, jobDataReplicator,
                readOnlyJobOperations, jobManagementClient, titusRuntime);

        ExecutionContext executionContext = ExecutionContext.newBuilder().withIteration(ExecutionId.initial()).build();
        StepVerifier.create(nodeConditionCtrl.handleNodesWithBadCondition(executionContext))
                .verifyComplete();

        assertThat(terminatedTaskIds).isNotEmpty();
        assertThat(terminatedTaskIds.size()).isEqualTo(2);
        verifyTerminatedTasksOnBadNodes(terminatedTaskIds, tasksByJobIdMap, nodeMap);
    }

    private void verifyTerminatedTasksOnBadNodes(Set<String> terminatedTaskIds,
                                                 Map<String, List<Task>> tasksByJobIdMap, Map<String, Node> nodeMap) {
        List<Task> allTasks = tasksByJobIdMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        List<String> badNodeIds = nodeMap.values().stream().filter(Node::isInBadCondition).map(Node::getId).collect(Collectors.toList());
        Set<String> taskIdsOnBadNodes = allTasks.stream()
                .filter(task -> task.getTaskContext().containsKey(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID) &&
                        badNodeIds.contains(task.getTaskContext().get(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID)))
                .map(Task::getId)
                .collect(Collectors.toSet());
        assertThat(taskIdsOnBadNodes).containsAll(terminatedTaskIds);
    }

    private List<Job<BatchJobExt>> getJobs() {
        Job<BatchJobExt> job1 = JobGenerator.batchJobsOfSize(2).getValue();
        Map<String, String> job2Attributes = new HashMap<>();
        job2Attributes.put(JobAttributes.JOB_PARAMETER_TERMINATE_ON_BAD_AGENT, "true");
        Job<BatchJobExt> job2 = JobGenerator.batchJobsOfSizeAndAttributes(2, job2Attributes).getValue();
        return Arrays.asList(job1, job2);
    }

    private List<Task> buildJobTasks(Job<BatchJobExt> batchJob, List<Node> nodes) {
        List<Task> tasksForJob = new ArrayList<>();
        List<BatchJobTask> batchTasks = JobGenerator.batchTasks(batchJob).getValues(nodes.size());
        for (int i = 0; i < batchTasks.size(); i++) {
            tasksForJob.add(batchTasks.get(i).toBuilder()
                    .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, nodes.get(i).getId())
                    .build());
        }
        return tasksForJob;
    }

    private Map<String, List<Task>> buildTasksForJobAndNodeAssignment(List<Node> nodes, List<Job<BatchJobExt>> jobs) {
        Map<String, List<Task>> tasksByJobIdMap = new HashMap<>(2);
        jobs.forEach(job -> tasksByJobIdMap.put(job.getId(), buildJobTasks(job, nodes)));
        return tasksByJobIdMap;
    }


    private Map<String, Node> buildNodes() {
        Map<String, Node> nodeMap = new HashMap<>(3);
        nodeMap.put(NodeIds.node1.name(), buildNode(NodeIds.node1.name(), true));
        nodeMap.put(NodeIds.node2.name(), buildNode(NodeIds.node2.name(), true));
        nodeMap.put(NodeIds.node3.name(), buildNode(NodeIds.node3.name(), false));
        return nodeMap;
    }

    private Node buildNode(String id, boolean isBadCondition) {
        return Node.newBuilder()
                .withServerGroupId("serverGroup1")
                .withId(id)
                .withBadCondition(isBadCondition)
                .build();
    }
}