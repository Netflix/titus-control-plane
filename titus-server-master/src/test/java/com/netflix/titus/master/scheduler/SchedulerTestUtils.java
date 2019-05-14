package com.netflix.titus.master.scheduler;

import java.util.HashMap;
import java.util.Map;

import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.TaskTrackerState;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QAttributes;
import com.netflix.titus.master.jobmanager.service.common.V3QueueableTask;
import org.apache.mesos.Protos;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchedulerTestUtils {
    public static VirtualMachineCurrentState createVirtualMachineCurrentStateMock(String id) {
        VirtualMachineCurrentState currentState = mock(VirtualMachineCurrentState.class);
        VirtualMachineLease lease = mock(VirtualMachineLease.class);
        Map<String, Protos.Attribute> attributes = new HashMap<>();
        attributes.put("id", Protos.Attribute.newBuilder().setName("id").setType(Protos.Value.Type.TEXT).setText(Protos.Value.Text.newBuilder().setValue(id)).build());
        when(lease.getAttributeMap()).thenReturn(attributes);
        when(currentState.getCurrAvailableResources()).thenReturn(lease);
        return currentState;
    }

    public static TaskRequest createTaskRequest() {
        V3QueueableTask taskRequest = mock(V3QueueableTask.class);
        QAttributes qAttributes = mock(QAttributes.class);
        when(qAttributes.getTierNumber()).thenReturn(1);
        when(taskRequest.getQAttributes()).thenReturn(qAttributes);
        return taskRequest;
    }

    public static TaskTrackerState createTaskTrackerState() {
        return mock(TaskTrackerState.class);
    }
}
