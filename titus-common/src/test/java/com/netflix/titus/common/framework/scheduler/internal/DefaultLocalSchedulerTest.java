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

package com.netflix.titus.common.framework.scheduler.internal;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.common.framework.scheduler.LocalScheduler;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.Schedule;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.framework.scheduler.model.ScheduledAction;
import com.netflix.titus.common.framework.scheduler.model.SchedulingStatus.SchedulingState;
import com.netflix.titus.common.framework.scheduler.model.event.LocalSchedulerEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleAddedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleRemovedEvent;
import com.netflix.titus.common.framework.scheduler.model.event.ScheduleUpdateEvent;
import com.netflix.titus.common.util.retry.Retryers;
import com.netflix.titus.common.util.time.Clocks;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class DefaultLocalSchedulerTest {

    private final AtomicReference<ScheduledAction> lastSucceededAction = new AtomicReference<>();
    private final AtomicReference<ScheduledAction> lastFailedAction = new AtomicReference<>();

    private final ScheduleDescriptor scheduleDescriptor = ScheduleDescriptor.newBuilder()
            .withName("testSchedule")
            .withDescription("Test scheduler")
            .withInterval(Duration.ofMillis(1))
            .withTimeout(Duration.ofMillis(500))
            .withRetryerSupplier(() -> Retryers.interval(100, TimeUnit.MILLISECONDS))
            .withOnSuccessHandler(lastSucceededAction::set)
            .withOnErrorHandler((action, error) -> lastFailedAction.set(action))
            .build();

    private final LocalScheduler localScheduler = new DefaultLocalScheduler(Duration.ofMillis(1), Schedulers.parallel(), Clocks.system(), false, new DefaultRegistry());

    private final TitusRxSubscriber<LocalSchedulerEvent> eventSubscriber = new TitusRxSubscriber<>();

    @Before
    public void setUp() {
        localScheduler.events().subscribe(eventSubscriber);
    }

    @Test
    public void testScheduleMono() throws Exception {
        AtomicLong tickCounter = new AtomicLong();
        ScheduleReference reference = localScheduler.scheduleMono(
                scheduleDescriptor.toBuilder().withName("testScheduleMono").build(),
                tick -> Mono.delay(Duration.ofMillis(1)).flatMap(t -> {
                    tickCounter.incrementAndGet();
                    return Mono.empty();
                }),
                Schedulers.parallel()
        );
        testExecutionLifecycle(reference, tickCounter);
    }

    @Test
    public void testScheduleAction() throws Exception {
        AtomicLong tickCounter = new AtomicLong();
        ScheduleReference reference = localScheduler.schedule(
                scheduleDescriptor.toBuilder().withName("testScheduleAction").build(),
                t -> tickCounter.incrementAndGet(),
                true
        );
        testExecutionLifecycle(reference, tickCounter);
    }

    private void testExecutionLifecycle(ScheduleReference reference, AtomicLong tickCounter) throws InterruptedException {
        // Schedule, and first iteration
        expectScheduleAdded(reference);

        expectScheduleUpdateEvent(SchedulingState.Running);
        expectScheduleUpdateEvent(SchedulingState.Succeeded);
        assertThat(tickCounter.get()).isGreaterThan(0);

        // Next running
        expectScheduleUpdateEvent(SchedulingState.Waiting);
        expectScheduleUpdateEvent(SchedulingState.Running);
        ScheduleUpdateEvent succeededEvent2 = expectScheduleUpdateEvent(SchedulingState.Succeeded);
        assertThat(succeededEvent2.getSchedule().getCompletedActions()).hasSize(1);

        // Now cancel it
        assertThat(reference.isClosed()).isFalse();
        reference.close();
        await().timeout(5, TimeUnit.SECONDS).until(reference::isClosed);

        assertThat(reference.isClosed()).isTrue();
        assertThat(reference.getSchedule().getCurrentAction().getStatus().getState().isFinal()).isTrue();
        assertThat(localScheduler.getActiveSchedules()).isEmpty();
        assertThat(localScheduler.getArchivedSchedules()).hasSize(1);

        expectScheduleRemoved(reference);
    }

    @Test
    public void testTimeout() throws Exception {
        ScheduleReference reference = localScheduler.scheduleMono(
                scheduleDescriptor.toBuilder().withName("testTimeout").build(),
                tick -> Mono.never(),
                Schedulers.parallel()
        );

        expectScheduleAdded(reference);
        expectScheduleUpdateEvent(SchedulingState.Running);
        expectScheduleUpdateEvent(SchedulingState.Failed);

        // Replacement
        expectScheduleUpdateEvent(SchedulingState.Waiting);

        assertThat(reference.getSchedule().getCompletedActions()).hasSize(1);
        ScheduledAction failedAction = reference.getSchedule().getCompletedActions().get(0);
        assertThat(failedAction.getStatus().getState()).isEqualTo(SchedulingState.Failed);
        assertThat(failedAction.getStatus().getError().get()).isInstanceOf(TimeoutException.class);
    }

    @Test
    public void testRetries() throws InterruptedException {
        AtomicInteger counter = new AtomicInteger();
        ScheduleReference reference = localScheduler.scheduleMono(
                scheduleDescriptor.toBuilder().withName("testRetries").build(),
                tick -> Mono.defer(() ->
                        counter.incrementAndGet() % 2 == 0
                                ? Mono.empty()
                                : Mono.error(new RuntimeException("Simulated error at iteration " + counter.get()))),
                Schedulers.parallel()
        );

        expectScheduleAdded(reference);
        expectScheduleUpdateEvent(SchedulingState.Running);
        expectScheduleUpdateEvent(SchedulingState.Failed);

        expectScheduleUpdateEvent(SchedulingState.Waiting);
        expectScheduleUpdateEvent(SchedulingState.Running);
        expectScheduleUpdateEvent(SchedulingState.Succeeded);
    }

    private void expectScheduleAdded(ScheduleReference reference) throws InterruptedException {
        assertThat(reference.isClosed()).isFalse();

        LocalSchedulerEvent addedEvent = eventSubscriber.takeNext(Duration.ofSeconds(5));
        assertThat(addedEvent).isInstanceOf(ScheduleAddedEvent.class);
        assertThat(addedEvent.getSchedule().getCurrentAction().getStatus().getState()).isEqualTo(SchedulingState.Waiting);

        assertThat(localScheduler.findSchedule(reference.getSchedule().getId())).isPresent();
    }

    private void expectScheduleRemoved(ScheduleReference reference) throws InterruptedException {
        assertThat(reference.isClosed()).isTrue();
        LocalSchedulerEvent removedEvent = eventSubscriber.takeUntil(e -> e instanceof ScheduleRemovedEvent, Duration.ofSeconds(5));

        Schedule schedule = removedEvent.getSchedule();
        assertThat(schedule.getCurrentAction().getStatus().getState().isFinal()).isTrue();

        assertThat(localScheduler.getArchivedSchedules()).hasSize(1);
        assertThat(localScheduler.getArchivedSchedules().get(0).getId()).isEqualTo(reference.getSchedule().getId());
    }

    private ScheduleUpdateEvent expectScheduleUpdateEvent(SchedulingState expectedState) throws InterruptedException {
        LocalSchedulerEvent event = eventSubscriber.takeNext(Duration.ofSeconds(5));
        assertThat(event).isInstanceOf(ScheduleUpdateEvent.class);
        assertThat(event.getSchedule().getCurrentAction().getStatus().getState()).isEqualTo(expectedState);
        return (ScheduleUpdateEvent) event;
    }
}