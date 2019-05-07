/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.ext.eureka.containerhealth;

import java.time.Duration;

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
import com.netflix.titus.testkit.model.job.JobComponentStub;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import static com.netflix.titus.api.jobmanager.model.job.JobFunctions.ofServiceSize;
import static com.netflix.titus.testkit.junit.asserts.ContainerHealthAsserts.assertContainerHealth;
import static com.netflix.titus.testkit.junit.asserts.ContainerHealthAsserts.assertContainerHealthAndEvent;
import static com.netflix.titus.testkit.junit.asserts.ContainerHealthAsserts.assertContainerHealthEvent;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.serviceJobDescriptors;
import static org.assertj.core.api.Assertions.assertThat;

public class EurekaContainerHealthServiceTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final EurekaServerStub eurekaServer = new EurekaServerStub();

    private final JobComponentStub jobManagerStub = new JobComponentStub(titusRuntime);

    private final ReadOnlyJobOperations jobOperations = jobManagerStub.getJobOperations();

    private final EurekaContainerHealthService healthService = new EurekaContainerHealthService(
            jobOperations, eurekaServer.getEurekaClient(), titusRuntime
    );

    private Job job1;
    private Task task1;
    private String taskId1;

    @Before
    public void setUp() {
        this.job1 = jobManagerStub.addServiceTemplate("testJob", serviceJobDescriptors(ofServiceSize(1)))
                .createJobAndTasks("testJob").getLeft();

        this.task1 = jobOperations.getTasks(job1.getId()).get(0);
        this.taskId1 = task1.getId();
    }

    @Test
    public void testJobManagerUpdate() {
        StepVerifier.create(healthService.events(false))
                // Task launched, but not in Eureka yet.
                .then(() -> jobManagerStub.moveTaskToState(taskId1, TaskState.Launched))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unknown))

                // Task started and registered with Eureka
                .then(() -> {
                    eurekaServer.register(newInstanceInfo(taskId1, InstanceStatus.UP));
                    jobManagerStub.moveTaskToState(taskId1, TaskState.Started);
                })
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                // Task terminated
                .then(() -> jobManagerStub.moveTaskToState(task1, TaskState.Finished))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Terminated))

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testEurekaUpdate() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Change state to UP
                .then(() -> registerAndRefresh(InstanceStatus.UP))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                // Change state to DOWN
                .then(() -> registerAndRefresh(InstanceStatus.DOWN))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unhealthy))

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testEurekaReRegistration() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Change state to UP
                .then(() -> registerAndRefresh(InstanceStatus.UP))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                // Unregister in Eureka
                .then(() -> {
                    eurekaServer.unregister(taskId1);
                    eurekaServer.triggerCacheRefreshUpdate();
                })
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unknown))

                // Register again
                .then(() -> registerAndRefresh(InstanceStatus.UP))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testOutOfServiceJobWithRealStateUp() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Change state to OUT_OF_SERVICE
                .then(() -> {
                    jobManagerStub.changeJobEnabledStatus(job1, false);
                    registerAndRefresh(InstanceStatus.OUT_OF_SERVICE);
                })
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testOutOfServiceJobWithRealStateDown() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Register first with DOWN state
                .then(() -> registerAndRefresh(InstanceStatus.DOWN))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unhealthy))

                // Disable job and change state to OUT_OF_SERVICE
                .then(() -> jobManagerStub.changeJobEnabledStatus(job1, false))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))
                .then(() -> registerAndRefresh(InstanceStatus.OUT_OF_SERVICE))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                // Enable job and remove OUT_OF_SERVICE override
                .then(() -> jobManagerStub.changeJobEnabledStatus(job1, true))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unhealthy))
                .then(() -> registerAndRefresh(InstanceStatus.UP))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                .thenCancel()
                .verify(Duration.ofSeconds(5_000));
    }

    @Test
    public void testOutOfServiceJobWithRealStateNotRegistered() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Check enabled job for not registered container
                .then(eurekaServer::triggerCacheRefreshUpdate)
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unknown))

                // Disable job
                .then(() -> jobManagerStub.changeJobEnabledStatus(job1, false))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Healthy))

                // Enable again
                .then(() -> jobManagerStub.changeJobEnabledStatus(job1, true))
                .assertNext(event -> assertContainerHealthAndEvent(event, healthService.getHealthStatus(taskId1), taskId1, ContainerHealthState.Unknown))

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testEurekaStaleDataCleanup() {
        jobManagerStub.moveTaskToState(taskId1, TaskState.Started);

        StepVerifier.create(healthService.events(false))
                // Start the task and register with Eureka
                .then(() -> registerAndRefresh(InstanceStatus.UP))
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
        TitusRxSubscriber<ContainerHealthEvent> subscriber1 = new TitusRxSubscriber<>();
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
        registerAndRefresh(InstanceStatus.DOWN);

        assertThat(subscription2.isDisposed()).isTrue();

        // Event 3
        registerAndRefresh(InstanceStatus.UP);

        assertThat(subscriber1.isDisposed()).isFalse();
        assertThat(subscriber1.getAllItems()).hasSize(3);
    }

    private void registerAndRefresh(InstanceStatus status) {
        eurekaServer.register(newInstanceInfo(taskId1, status));
        eurekaServer.triggerCacheRefreshUpdate();
    }

    private InstanceInfo newInstanceInfo(String taskId, InstanceStatus instanceStatus) {
        Pair<Job<?>, Task> jobTaskPair = jobOperations.findTaskById(taskId).orElseThrow(() -> new IllegalStateException("Task not found: " + taskId));
        return EurekaGenerator.newTaskInstanceInfo(jobTaskPair.getLeft(), jobTaskPair.getRight(), instanceStatus);
    }
}