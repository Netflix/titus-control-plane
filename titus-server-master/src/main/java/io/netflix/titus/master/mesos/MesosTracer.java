package io.netflix.titus.master.mesos;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.netflix.fenzo.VirtualMachineLease;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MesosTracer {

    private static final Logger logger = LoggerFactory.getLogger(MesosTracer.class);

    static String toTaskIds(List<Protos.TaskInfo> taskInfos) {
        return taskInfos.stream().map(taskInfo -> taskInfo.getTaskId().getValue()).collect(Collectors.joining(","));
    }

    static String toLeaseIds(List<VirtualMachineLease> leases) {
        return leases.stream().map(VirtualMachineLease::getId).collect(Collectors.joining(","));
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
            logger.info("[Mesos/Request] OK    {} (execution time {}ms)", message, stopwatch.elapsed(TimeUnit.MILLISECONDS));
            return result;
        } catch (Exception e) {
            logger.info("[Mesos/Request] ERROR {} (execution time {}ms). Failure message: {}", message, stopwatch.elapsed(TimeUnit.MILLISECONDS), e.getMessage());
            throw e;
        }
    }

    static void traceMesosCallback(String message) {
        logger.info("[Mesos/Callback] {}", message);
    }
}
