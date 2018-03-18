package io.netflix.titus.master.scheduler;

import java.util.Map;
import java.util.Optional;

import com.google.common.base.Strings;
import com.netflix.fenzo.VirtualMachineCurrentState;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.queues.QueuableTask;
import org.apache.mesos.Protos;

public class SchedulerUtils {

    public static boolean hasGpuRequest(ScheduledRequest request) {
        return hasGpuRequest((QueuableTask) request);
    }

    public static boolean hasGpuRequest(QueuableTask task) {
        return task != null && task.getScalarRequests() != null &&
                task.getScalarRequests().get("gpu") != null &&
                task.getScalarRequests().get("gpu") >= 1.0;
    }

    public static Optional<String> getInstanceGroupName(String instanceGroupAttributeName, VirtualMachineLease lease) {
        Map<String, Protos.Attribute> attributeMap = lease.getAttributeMap();
        if (attributeMap != null) {
            final Protos.Attribute attribute = attributeMap.get(instanceGroupAttributeName);
            if (attribute != null && attribute.hasText()) {
                return Optional.of(attribute.getText().getValue());
            }
        }
        return Optional.empty();
    }

    public static String getAttributeValue(VirtualMachineCurrentState targetVM, String attributeName) {
        Protos.Attribute attribute = targetVM.getCurrAvailableResources().getAttributeMap().get(attributeName);
        return Strings.nullToEmpty(attribute.getText().getValue());
    }
}
