package com.netflix.titus.supplementary.relocation.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.jobmanager.JobAttributes;
import com.netflix.titus.api.jobmanager.TaskAttributes;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import reactor.core.publisher.Mono;

import static com.netflix.titus.common.data.generator.DataGenerator.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeConditionCtrlTest {

    enum NodeIds {
        node1,
        node2,
        node3
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

        NodeConditionCtrl nodeConditionCtrl = new NodeConditionCtrl(configuration, nodeDataResolver, jobDataReplicator,
                readOnlyJobOperations, jobManagementClient, titusRuntime);

        ExecutionContext executionContext = ExecutionContext.newBuilder().withIteration(ExecutionId.initial()).build();
        nodeConditionCtrl.handleBadNodeConditions(executionContext).block();

        assertThat(terminatedTaskIds).isNotEmpty();
    }

    private List<Job<BatchJobExt>> getJobs() {
        Job<BatchJobExt> job1 = JobGenerator.batchJobsOfSize(2).getValue();

        Map<String, String> job2Attributes = new HashMap<>();
        job2Attributes.put(JobAttributes.JOB_PARAMETER_TERMINATE_ON_BAD_AGENT, "true");
        Job<BatchJobExt> job2 = JobGenerator.batchJobsOfSizeAndAttributes(2, job2Attributes).getValue();

        return Arrays.asList(job1, job2);
    }

    private Task buildTask(Job<BatchJobExt> batchJob, String assignedNodeId) {
        Map<String, String> taskAttributes = new HashMap<>();
        taskAttributes.put(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, assignedNodeId);
        return JobGenerator.batchTasksWithAttributes(batchJob, taskAttributes).getValue();
    }

    private Map<String, List<Task>> buildTasksForJobAndNodeAssignment(List<Node> nodes, List<Job<BatchJobExt>> jobs) {
        Map<String, List<Task>> tasksByJobIdMap = new HashMap<>(2);
        jobs.forEach(job -> tasksByJobIdMap.put(job.getId(), new ArrayList<>()));

        nodes.forEach(node -> jobs.forEach(job -> tasksByJobIdMap.get(job.getId()).add(buildTask(job, node.getId()))));

        return tasksByJobIdMap;
    }


    private Map<String, Node> buildNodes() {
        Map<String, Node> nodeMap = new HashMap<>(3);
        nodeMap.put(NodeIds.node1.name(), buildNode(NodeIds.node1.name(), true));
        nodeMap.put(NodeIds.node2.name(), buildNode(NodeIds.node2.name(), true));
        nodeMap.put(NodeIds.node3.name(), buildNode(NodeIds.node3.name(), true));
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