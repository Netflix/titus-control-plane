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

package com.netflix.titus.master.service.management.internal;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.netflix.spectator.api.DefaultRegistry;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.master.service.management.CompositeResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupRemovedEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupUndefinedEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.ResourceConsumptionEvent;
import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultResourceConsumptionServiceTest {

    private static final String NOT_DEFINED_CAPACITY_GROUP = "not_defined_capacity_group";

    private final TestScheduler testScheduler = Schedulers.test();

    private final AtomicReference<DefaultResourceConsumptionService.ConsumptionEvaluationResult> lastEvaluation = new AtomicReference<>();
    private final Supplier<DefaultResourceConsumptionService.ConsumptionEvaluationResult> evaluator = lastEvaluation::get;

    private final DefaultResourceConsumptionService resourceConsumptionService = new DefaultResourceConsumptionService(
            evaluator, new DefaultRegistry(), testScheduler
    );

    private final ConsumptionModelGenerator generator = new ConsumptionModelGenerator();

    private final ExtTestSubscriber<ResourceConsumptionEvent> testSubscriber = new ExtTestSubscriber<>();

    @Before
    public void setUp() throws Exception {
        resourceConsumptionService.enterActiveMode().subscribe();
        resourceConsumptionService.resourceConsumptionEvents().subscribe(testSubscriber);
    }

    @After
    public void tearDown() throws Exception {
        resourceConsumptionService.shutdown();
    }

    @Test
    public void testGetSystemConsumption() throws Exception {
        lastEvaluation.set(generator.getEvaluation());
        testScheduler.advanceTimeBy(DefaultResourceConsumptionService.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        Optional<CompositeResourceConsumption> resultOpt = resourceConsumptionService.getSystemConsumption();
        assertThat(resultOpt).isNotEmpty();
    }

    @Test
    public void testCapacityConsumptionEvents() throws Exception {
        lastEvaluation.set(generator.getEvaluation());
        testScheduler.advanceTimeBy(DefaultResourceConsumptionService.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        List<ResourceConsumptionEvent> events = testSubscriber.getOnNextItems();
        assertThat(events).hasSize(generator.getDefinedCapacityGroupNames().size());
    }

    @Test
    public void testUndefinedCapacityGroupNameEvents() throws Exception {
        generator.addConsumption(NOT_DEFINED_CAPACITY_GROUP, "myApp", new ResourceDimension(1, 0, 1, 1, 1), new ResourceDimension(2, 0, 2, 2, 2));
        lastEvaluation.set(generator.getEvaluation());
        testScheduler.advanceTimeBy(DefaultResourceConsumptionService.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        List<ResourceConsumptionEvent> events = testSubscriber.getOnNextItems();
        assertThat(events).hasSize(generator.getDefinedCapacityGroupNames().size() + 2);

        Optional<CapacityGroupUndefinedEvent> event = ConsumptionModelGenerator.findEvent(events, CapacityGroupUndefinedEvent.class, NOT_DEFINED_CAPACITY_GROUP);
        assertThat(event).isNotEmpty();
    }

    @Test
    public void testRemovedCapacityGroupNameEvents() throws Exception {
        lastEvaluation.set(generator.getEvaluation());
        testScheduler.advanceTimeBy(DefaultResourceConsumptionService.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        List<ResourceConsumptionEvent> initialEvents = testSubscriber.getOnNextItems();

        // Now remove one capacity group and trigger another evaluation
        generator.removeCapacityGroup(ConsumptionModelGenerator.CRITICAL_SLA_1.getAppName());
        lastEvaluation.set(generator.getEvaluation());
        testScheduler.advanceTimeBy(DefaultResourceConsumptionService.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        List<ResourceConsumptionEvent> allEvents = testSubscriber.getOnNextItems();
        List<ResourceConsumptionEvent> newEvents = allEvents.subList(initialEvents.size(), allEvents.size());

        assertThat(newEvents).hasSize(1); // For the removed capacity group
        assertThat(newEvents.get(0)).isInstanceOf(CapacityGroupRemovedEvent.class);
        assertThat(newEvents.get(0).getCapacityGroup()).isEqualTo(ConsumptionModelGenerator.CRITICAL_SLA_1.getAppName());
    }

    @Test
    public void testEventsAreNotDuplicated() throws Exception {
        lastEvaluation.set(generator.getEvaluation());
        testScheduler.advanceTimeBy(DefaultResourceConsumptionService.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);

        List<ResourceConsumptionEvent> initialEvents = testSubscriber.getOnNextItems();

        // Evaluate again, without changing anything
        testScheduler.advanceTimeBy(DefaultResourceConsumptionService.UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        List<ResourceConsumptionEvent> allEvents = testSubscriber.getOnNextItems();

        assertThat(allEvents.size()).isEqualTo(initialEvents.size());
    }
}