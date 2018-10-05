package com.netflix.titus.ext.eureka.containerhealth;

import java.time.Duration;
import java.util.Collection;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.model.job.TaskState;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.ext.eureka.EurekaGenerator;
import com.netflix.titus.ext.eureka.EurekaServerStub;
import com.netflix.titus.testkit.model.job.JobGeneratorOrchestrator;
import io.reactivex.subscribers.TestSubscriber;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import static com.netflix.titus.testkit.junit.asserts.ContainerHealthAsserts.assertContainerHealth;
import static com.netflix.titus.testkit.junit.asserts.ContainerHealthAsserts.assertContainerHealthEvent;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.ofBatchSize;
import static org.assertj.core.api.Assertions.assertThat;

public class EurekaContainerHealthServiceTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final EurekaServerStub eurekaServer = new EurekaServerStub();

    private final JobGeneratorOrchestrator jobManagerStub = new JobGeneratorOrchestrator(titusRuntime);

    private final ReadOnlyJobOperations jobOperations = jobManagerStub.getReadOnlyJobOperations();

    private final EurekaContainerHealthService healthService = new EurekaContainerHealthService(
            jobOperations, eurekaServer.getEurekaClient(), titusRuntime
    );

    private Job job1;
    private Task task1;
    private String taskId1;

    @Before
    public void setUp() {
        this.job1 = jobManagerStub.addBatchTemplate("testJob", batchJobDescriptors(ofBatchSize(1)))
                .createJobAndTasks("testJob").getLeft();

        this.task1 = jobOperations.getTasks(job1.getId()).get(0);
        this.taskId1 = task1.getId();
    }

    @Test
    public void testJobManagerUpdate() {
        StepVerifier.create(healthService.events(false))
                // Task launched, but not in Eureka yet.
                .then(() -> jobManagerStub.moveTaskToState(taskId1, TaskState.Launched))
                .assertNext(event -> {
                    assertContainerHealth(healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unknown);
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Unknown);
                })

                // Task started and registered with Eureka
                .then(() -> {
                    eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.UP));
                    jobManagerStub.moveTaskToState(taskId1, TaskState.Started);
                })
                .assertNext(event -> {
                    assertContainerHealth(healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy);
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Healthy);
                })

                // Task terminated
                .then(() -> jobManagerStub.moveTaskToState(task1, TaskState.Finished))
                .assertNext(event -> {
                    assertContainerHealth(healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Terminated);
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Terminated);
                })

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testEurekaUpdate() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Change state to UP
                .then(() -> {
                    eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.UP));
                    eurekaServer.triggerCacheRefreshUpdate();
                })
                .assertNext(event -> {
                    assertContainerHealth(healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy);
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Healthy);
                })
                // Change state to DOWN
                .then(() -> {
                    eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.DOWN));
                    eurekaServer.triggerCacheRefreshUpdate();
                })
                .assertNext(event -> {
                    assertContainerHealth(healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unhealthy);
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Unhealthy);
                })

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testEurekaStaleDataCleanup() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Start the task and register with Eureka
                .then(() -> {
                    eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.UP));
                    eurekaServer.triggerCacheRefreshUpdate();
                })
                .assertNext(event -> assertContainerHealthEvent(event, taskId1, ContainerHealthState.Healthy))

                // Lose task
                .then(() -> {
                    jobManagerStub.forget(task1);
                    eurekaServer.triggerCacheRefreshUpdate();
                })
                .assertNext(event -> {
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Terminated);
                    assertThat(healthService.findHealthStatus(taskId1)).isEmpty();
                })

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testBadSubscriberIsolation() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);
        eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.UP));

        // First event / one subscriber
        TestSubscriber<ContainerHealthEvent> subscriber1 = new TestSubscriber<>();
        healthService.events(false).subscribe(subscriber1);

        eurekaServer.triggerCacheRefreshUpdate();

        // Add bad subscriber
        Disposable subscription2 = healthService.events(false).subscribe(
                next -> {
                    throw new RuntimeException("simulated error");
                },
                e -> {
                    throw new RuntimeException("simulated error");
                },
                () -> {
                    throw new RuntimeException("simulated error");
                }
        );

        // Event 2
        eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.DOWN));
        eurekaServer.triggerCacheRefreshUpdate();

        assertThat(subscription2.isDisposed()).isTrue();

        // Event 3
        eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.UP));
        eurekaServer.triggerCacheRefreshUpdate();

        assertThat(subscriber1.isDisposed()).isFalse();
        assertThat(subscriber1.getEvents().stream().flatMap(Collection::stream)).hasSize(3);
    }

    private InstanceInfo newInstanceInfo(String taskId, InstanceStatus instanceStatus) {
        Pair<Job<?>, Task> jobTaskPair = jobOperations.findTaskById(taskId).orElseThrow(() -> new IllegalStateException("Task not found: " + taskId));
        return EurekaGenerator.newTaskInstanceInfo(jobTaskPair.getLeft(), jobTaskPair.getRight(), instanceStatus);
    }
}