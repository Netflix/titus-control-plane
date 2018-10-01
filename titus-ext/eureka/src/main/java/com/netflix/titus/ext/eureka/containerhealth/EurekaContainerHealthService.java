package com.netflix.titus.ext.eureka.containerhealth;

import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.event.TaskUpdateEvent;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.rx.ReactorExt;
import reactor.core.publisher.Flux;

@Singleton
public class EurekaContainerHealthService implements ContainerHealthService {

    public static final String NAME = "eureka";

    private final ReadOnlyJobOperations jobOperations;
    private final EurekaClient eurekaClient;

    @Inject
    public EurekaContainerHealthService(ReadOnlyJobOperations jobOperations, EurekaClient eurekaClient) {
        this.jobOperations = jobOperations;
        this.eurekaClient = eurekaClient;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
        return jobOperations.findTaskById(taskId).map(jobAndTaskPair -> buildHealthStatus(jobAndTaskPair.getRight()));
    }

    @Override
    public Flux<ContainerHealthEvent> events(boolean snapshot) {
        return ReactorExt.toFlux(jobOperations.observeJobs()).flatMap(event -> {
            if (event instanceof TaskUpdateEvent) {
                TaskUpdateEvent taskUpdateEvent = (TaskUpdateEvent) event;
                Task task = taskUpdateEvent.getCurrentTask();
                return Flux.just(ContainerHealthEvent.healthChanged(buildHealthStatus(task)));
            }
            return Flux.empty();
        });
    }

    private ContainerHealthStatus buildHealthStatus(Task task) {
        List<InstanceInfo> instances = eurekaClient.getInstancesById(task.getId());

        ContainerHealthState state;
        if (CollectionsExt.isNullOrEmpty(instances)) {
            state = ContainerHealthState.Unknown;
        } else {
            InstanceInfo instance = instances.get(0);
            state = instance.getStatus() == InstanceInfo.InstanceStatus.UP
                    ? ContainerHealthState.Healthy
                    : ContainerHealthState.Unhealthy;
        }

        return ContainerHealthStatus.newBuilder()
                .withTaskId(task.getId())
                .withTimestamp(System.currentTimeMillis())
                .withState(state).build();
    }
}
