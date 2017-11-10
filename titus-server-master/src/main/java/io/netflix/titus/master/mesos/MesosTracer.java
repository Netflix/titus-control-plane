package io.netflix.titus.master.mesos;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.protobuf.TextFormat;
import com.netflix.fenzo.VirtualMachineLease;
import io.titanframework.messages.TitanProtos;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MesosTracer {

    private static final Logger logger = LoggerFactory.getLogger("TitusMesosLog");

    static String toTaskSummary(Protos.TaskInfo taskInfo) {
        StringBuilder sb = new StringBuilder(taskInfo.getTaskId().getValue()).append('{');

        double cpu = 0.0;
        double memory = 0.0;
        double disk = 0.0;
        double network = 0.0;
        for (Protos.Resource r : taskInfo.getResourcesList()) {
            if (r.getType() == Protos.Value.Type.SCALAR) {
                String name = r.getName();
                double value = r.getScalar().getValue();
                if (name.equals("cpus")) {
                    cpu = value;
                } else if (name.equals("mem")) {
                    memory = value;
                } else if (name.equals("disk")) {
                    disk = value;
                } else if (name.equals("network")) {
                    network = value;
                }
            }
        }

        int gpu;
        String containerInfoSummary;
        try {
            TitanProtos.ContainerInfo containerInfo = TitanProtos.ContainerInfo.parseFrom(taskInfo.getData());
            if (containerInfo.getNetworkConfigInfo() == null) {
                containerInfoSummary = "ipNotAssigned";
            } else {
                containerInfoSummary = TextFormat.shortDebugString(containerInfo.getNetworkConfigInfo());
            }
            gpu = containerInfo.getNumGpus();

        } catch (Exception e) {
            containerInfoSummary = "containerInfo error: " + e.getMessage();
            gpu = 0;
        }

        sb.append("cpu=").append(cpu).append(',');
        sb.append("gpu=").append(gpu).append(',');
        sb.append("memory=").append(memory).append(',');
        sb.append("disk=").append(disk).append(',');
        sb.append("network=").append(network).append(',');
        sb.append("containerInfo=").append(containerInfoSummary).append(',');

        sb.append('}');
        return sb.toString();
    }

    static String toTaskSummary(List<Protos.TaskInfo> taskInfos) {
        return taskInfos.stream().map(MesosTracer::toTaskSummary).collect(Collectors.joining(","));
    }

    static String toLeaseIds(List<VirtualMachineLease> leases) {
        return leases.stream()
                .map(virtualMachineLease -> virtualMachineLease.hostname() + '/' + virtualMachineLease.getId())
                .collect(Collectors.joining(","));
    }

    static void traceMesosVoidRequest(String message, Runnable runnable) {
        traceMesosRequest(message, () -> {
            runnable.run();
            return null;
        });
    }

    static <T> T traceMesosRequest(String message, Supplier<T> callable) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            T result = callable.get();
            logger.debug("[Mesos/Request] OK {} (execution time {}ms)", message, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            return result;
        } catch (Exception e) {
            logger.error("[Mesos/Request] ERROR {} (execution time {}ms). Failure message: {}", message, stopwatch.elapsed(TimeUnit.MILLISECONDS), e.getMessage());
            throw e;
        }
    }

    static void logMesosCallbackDebug(String message, Object... args) {
        if (args.length == 0) {
            logger.debug("[Mesos/Callback] {}", message);
        } else {
            logger.debug("[Mesos/Callback] {}", String.format(message, args));
        }
    }

    static void logMesosCallbackInfo(String message, Object... args) {
        if (args.length == 0) {
            logger.info("[Mesos/Callback] {}", message);
        } else {
            logger.info("[Mesos/Callback] {}", String.format(message, args));
        }
    }

    static void logMesosCallbackWarn(String message, Object... args) {
        if (args.length == 0) {
            logger.warn("[Mesos/Callback] {}", message);
        } else {
            logger.warn("[Mesos/Callback] {}", String.format(message, args));
        }
    }

    static void logMesosCallbackError(String message, Object... args) {
        if (args.length == 0) {
            logger.error("[Mesos/Callback] {}", message);
        } else {
            logger.error("[Mesos/Callback] {}", String.format(message, args));
        }
    }
}
