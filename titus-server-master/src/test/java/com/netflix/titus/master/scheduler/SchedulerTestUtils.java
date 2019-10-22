package com.netflix.titus.master.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.netflix.fenzo.AssignableVirtualMachine;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.titus.api.agent.model.AgentInstance;
import com.netflix.titus.api.agent.model.AgentInstanceGroup;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleState;
import com.netflix.titus.api.agent.model.InstanceGroupLifecycleStatus;
import com.netflix.titus.api.agent.model.InstanceLifecycleState;
import com.netflix.titus.api.agent.model.InstanceLifecycleStatus;
import com.netflix.titus.api.jobmanager.model.job.BatchJobTask;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.model.job.TaskStatus;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.apache.mesos.Protos;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchedulerTestUtils {

    public static final String TASK_ID = "task-1234";
    public static final String INSTANCE_ID = "1234";
    public static final String INSTANCE_GROUP_ID = "instanceGroupId";
    public static final String UNKNOWN_INSTANCE_GROUP_ID = "unknownInstanceGroupId";

    public static VirtualMachineCurrentState createVirtualMachineCurrentStateMock(String id) {
        VirtualMachineCurrentState currentState = mock(VirtualMachineCurrentState.class);
        VirtualMachineLease lease = mock(VirtualMachineLease.class);
        Map<String, Protos.Attribute> attributes = new HashMap<>();
        attributes.put("id", Protos.Attribute.newBuilder().setName("id").setType(Protos.Value.Type.TEXT).setText(Protos.Value.Text.newBuilder().setValue(id)).build());
        when(lease.getAttributeMap()).thenReturn(attributes);
        when(currentState.getCurrAvailableResources()).thenReturn(lease);
        return currentState;
    }

    public static TaskTrackerState createTaskTrackerState() {
        return mock(TaskTrackerState.class);
    }

    public static VirtualMachineCurrentState createVirtualMachineCurrentStateMock(String id, List<TaskRequest> runningTasks,
                                                                                  List<TaskAssignmentResult> assignedTasks) {
        VirtualMachineCurrentState currentState = mock(VirtualMachineCurrentState.class);
        VirtualMachineLease lease = mock(VirtualMachineLease.class);
        Map<String, Protos.Attribute> attributes = new HashMap<>();
        attributes.put("id", Protos.Attribute.newBuilder().setName("id").setType(Protos.Value.Type.TEXT).setText(Protos.Value.Text.newBuilder().setValue(id)).build());
        when(lease.getAttributeMap()).thenReturn(attributes);
        when(currentState.getCurrAvailableResources()).thenReturn(lease);
        when(currentState.getRunningTasks()).thenReturn(runningTasks);
        when(currentState.getTasksCurrentlyAssigned()).thenReturn(assignedTasks);
        return currentState;
    }

    public static TaskRequest createTaskRequest(String id) {
        return createTaskRequest(id, null, null);
    }

    public static TaskRequest createTaskRequest(String id, Job job, Task task) {
        V3QueueableTask taskRequest = mock(V3QueueableTask.class);
        QAttributes qAttributes = mock(QAttributes.class);
        when(taskRequest.getId()).thenReturn(id);
        when(qAttributes.getTierNumber()).thenReturn(1);
        when(taskRequest.getQAttributes()).thenReturn(qAttributes);
        when(taskRequest.getJob()).thenReturn(job);
        when(taskRequest.getTask()).thenReturn(task);
        return taskRequest;
    }

    public static Task createStartLaunchedTask(String id) {
        BatchJobTask batchJobTask = JobGenerator.oneBatchTask();
        return batchJobTask.toBuilder()
                .withId(id)
                .withStatus(TaskStatus.newBuilder().withState(TaskState.Launched).build())
                .build();
    }

    public static List<TaskAssignmentResult> createMockAssignmentResultList(int count) {
        List<TaskAssignmentResult> results = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            results.add(mock(TaskAssignmentResult.class));
        }

        return results;
    }


    public static AssignableVirtualMachine createAssignableVirtualMachineMock(VirtualMachineCurrentState currentState) {
        AssignableVirtualMachine machine = mock(AssignableVirtualMachine.class);
        when(machine.getVmCurrentState()).thenReturn(currentState);
        return machine;
    }

    public static AgentInstanceGroup createAgentInstanceGroup(String instanceGroupId, InstanceGroupLifecycleState state,
                                                              Tier tier) {
        return createAgentInstanceGroup(instanceGroupId, state, tier, 0, Collections.emptyMap());
    }

    public static AgentInstanceGroup createAgentInstanceGroup(String instanceGroupId, InstanceGroupLifecycleState state,
                                                              Tier tier, Map<String, String> attributes) {
        return createAgentInstanceGroup(instanceGroupId, state, tier, 0, attributes);
    }

    public static AgentInstanceGroup createAgentInstanceGroup(String instanceGroupId, InstanceGroupLifecycleState state,
                                                              Tier tier, int gpus) {
        return createAgentInstanceGroup(instanceGroupId, state, tier, gpus, Collections.emptyMap());
    }

    public static AgentInstanceGroup createAgentInstanceGroup(String instanceGroupId, InstanceGroupLifecycleState state,
                                                              Tier tier, int gpus, Map<String, String> attributes) {
        ResourceDimension resourceDimension = ResourceDimension.newBuilder()
                .withCpus(1)
                .withMemoryMB(4096)
                .withDiskMB(10000)
                .withNetworkMbs(128)
                .withGpu(gpus)
                .build();
        return AgentInstanceGroup.newBuilder()
                .withId(instanceGroupId)
                .withResourceDimension(resourceDimension)
                .withLifecycleStatus(InstanceGroupLifecycleStatus.newBuilder().withState(state).build())
                .withTier(tier)
                .withTimestamp(System.currentTimeMillis())
                .withAttributes(attributes)
                .build();
    }

    public static AgentInstance createAgentInstance(String instanceId, String instanceGroupId) {
        return createAgentInstance(instanceId, instanceGroupId, Collections.emptyMap());
    }

    public static AgentInstance createAgentInstance(String instanceId, String instanceGroupId, Map<String, String> attributes) {
        return AgentInstance.newBuilder()
                .withId(instanceId)
                .withInstanceGroupId(instanceGroupId)
                .withDeploymentStatus(InstanceLifecycleStatus.newBuilder().withState(InstanceLifecycleState.Started).build())
                .withAttributes(attributes)
                .build();
    }
}
