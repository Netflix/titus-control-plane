package com.netflix.titus.supplementary.relocation.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.netflix.titus.api.jobmanager.TaskAttributes;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.ext.BatchJobExt;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.supplementary.relocation.connector.TitusNode;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelocationUtilTest {

    private TitusNode buildNode(String nodeId) {
        return TitusNode.newBuilder()
                .withId(nodeId)
                .withServerGroupId("serverGroup1")
                .build();
    }

    @Test
    public void buildTasksFromNodesAndJobsFilter() {
        String node1 = "node1";
        String node2 = "node2";
        String node3 = "node3";

        Job<BatchJobExt> job1 = JobGenerator.oneBatchJob();
        Job<BatchJobExt> job2 = JobGenerator.oneBatchJob();
        Job<BatchJobExt> job3 = JobGenerator.oneBatchJob();

        BatchJobTask task1 = JobGenerator.batchTasks(job1).getValue().toBuilder()
                .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, node1).build();

        BatchJobTask task2 = JobGenerator.batchTasks(job2).getValue().toBuilder()
                .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, node2).build();

        BatchJobTask task3 = JobGenerator.batchTasks(job3).getValue().toBuilder()
                .addToTaskContext(TaskAttributes.TASK_ATTRIBUTES_AGENT_INSTANCE_ID, node3).build();

        ReadOnlyJobOperations jobOperations = mock(ReadOnlyJobOperations.class);
        when(jobOperations.getJobs()).thenReturn(Arrays.asList(job1, job2, job3));
        when(jobOperations.getTasks(job1.getId())).thenReturn(Collections.singletonList(task1));
        when(jobOperations.getTasks(job2.getId())).thenReturn(Collections.singletonList(task2));
        when(jobOperations.getTasks(job3.getId())).thenReturn(Collections.singletonList(task3));

        Map<String, TitusNode> nodes = new HashMap<>(3);
        nodes.put(node1, buildNode(node1));
        nodes.put(node2, buildNode(node2));
        nodes.put(node3, buildNode(node3));

        Set<String> jobIds = new HashSet<>(2);
        jobIds.addAll(Arrays.asList(job1.getId(), job3.getId()));

        List<String> taskIdsOnBadNodes = RelocationUtil.buildTasksFromNodesAndJobsFilter(nodes, jobIds, jobOperations);
        assertThat(taskIdsOnBadNodes.size()).isEqualTo(2);
    }
}