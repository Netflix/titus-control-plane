package com.netflix.titus.runtime.containerhealth.service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.containerhealth.model.ContainerHealthState;
import com.netflix.titus.api.containerhealth.model.ContainerHealthStatus;
import com.netflix.titus.api.containerhealth.model.event.ContainerHealthEvent;
import com.netflix.titus.api.containerhealth.service.ContainerHealthService;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.Task;
import com.netflix.titus.api.jobmanager.service.ReadOnlyJobOperations;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.model.job.JobGeneratorOrchestrator;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static com.netflix.titus.common.util.CollectionsExt.asSet;
import static com.netflix.titus.testkit.junit.asserts.ContainerHealthAsserts.assertContainerHealthEvent;
import static com.netflix.titus.testkit.junit.asserts.ContainerHealthAsserts.assertContainerHealthSnapshot;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.batchJobDescriptors;
import static com.netflix.titus.testkit.model.job.JobDescriptorGenerator.ofBatchSize;
import static org.assertj.core.api.Assertions.assertThat;

public class AggregatingContainerHealthServiceTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.test();

    private final JobGeneratorOrchestrator jobManagerStub = new JobGeneratorOrchestrator(titusRuntime);

    private final ReadOnlyJobOperations jobOperations = jobManagerStub.getReadOnlyJobOperations();

    private final DownstreamHealthService downstream1 = new DownstreamHealthService("downstream1");
    private final DownstreamHealthService downstream2 = new DownstreamHealthService("downstream2");

    private final AggregatingContainerHealthService healthService = new AggregatingContainerHealthService(
            asSet(downstream1, downstream2),
            jobOperations,
            asSet("downstream1", "downstream2"),
            titusRuntime
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
    public void testSubscriptionWithSnapshot() {
        downstream1.makeHealthy(taskId1);
        downstream2.makeHealthy(taskId1);

        StepVerifier.create(healthService.events(true))
                // Check snapshot
                .assertNext(event -> assertContainerHealthSnapshot(
                        event, status -> status.getTaskId().equals(taskId1) && status.getState() == ContainerHealthState.Healthy)
                )

                // Now trigger change
                .then(() -> downstream1.makeUnhealthy(taskId1))
                .assertNext(event -> assertContainerHealthEvent(event, taskId1, ContainerHealthState.Unhealthy))

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testHealthStatusMergeFromDifferentSources() {
        downstream1.makeHealthy(taskId1);
        downstream2.makeHealthy(taskId1);

        StepVerifier.withVirtualTime(() -> healthService.events(false))
                .expectSubscription()

                // Check no, snapshot
                .expectNoEvent(Duration.ofSeconds(1))

                // Now trigger change
                .then(() -> {
                    assertThat(healthService.findHealthStatus(taskId1).get().getState()).isEqualTo(ContainerHealthState.Healthy);
                    downstream1.makeUnhealthy(taskId1);
                })
                .assertNext(event -> {
                    assertThat(healthService.findHealthStatus(taskId1).get().getState()).isEqualTo(ContainerHealthState.Unhealthy);
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Unhealthy);
                })

                // Now trigger it back
                .then(() -> downstream1.makeHealthy(taskId1))
                .assertNext(event -> {
                    assertThat(healthService.findHealthStatus(taskId1).get().getState()).isEqualTo(ContainerHealthState.Healthy);
                    assertContainerHealthEvent(event, taskId1, ContainerHealthState.Healthy);
                })

                .thenCancel()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    public void testBadHealthDownstreamSourceTerminatesClientSubscription() {
        StepVerifier.create(healthService.events(false))
                // Break downstream health provider
                .then(() -> {
                    downstream2.breakSubscriptionsWithError(new RuntimeException("Simulated error"));
                })

                .verifyError(RuntimeException.class);
    }

    @Test
    public void testBadSubscriberIsIsolated() {
        // First event / one subscriber
        TitusRxSubscriber<ContainerHealthEvent> goodSubscriber = new TitusRxSubscriber<>();
        healthService.events(false).subscribe(goodSubscriber);

        // Add bad subscriber
        Disposable badSubscriber = healthService.events(false).subscribe(
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

        downstream1.makeHealthy(taskId1);
        assertThat(goodSubscriber.isOpen()).isTrue();
        assertThat(badSubscriber.isDisposed()).isTrue();
    }

    private class DownstreamHealthService implements ContainerHealthService {

        private final String name;
        private final Map<String, ContainerHealthStatus> healthStatuses = new HashMap<>();

        private volatile DirectProcessor<ContainerHealthEvent> eventSubject = DirectProcessor.create();

        private DownstreamHealthService(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public Optional<ContainerHealthStatus> findHealthStatus(String taskId) {
            return Optional.ofNullable(healthStatuses.get(taskId));
        }

        @Override
        public Flux<ContainerHealthEvent> events(boolean snapshot) {
            return Flux.defer(() -> eventSubject);
        }

        private void makeHealthy(String taskId) {
            updateHealth(ContainerHealthStatus.healthy(taskId, 0));
        }

        private void makeUnhealthy(String taskId) {
            updateHealth(ContainerHealthStatus.unhealthy(taskId, 0));
        }

        private void updateHealth(ContainerHealthStatus newStatus) {
            healthStatuses.put(newStatus.getTaskId(), newStatus);
            eventSubject.onNext(ContainerHealthEvent.healthChanged(newStatus));
        }

        private void breakSubscriptionsWithError(RuntimeException error) {
            DirectProcessor<ContainerHealthEvent> current = eventSubject;
            this.eventSubject = DirectProcessor.create();
            current.onError(error);
        }
    }
}