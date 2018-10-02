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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.jobmanager.service.V3JobOperations;
import com.netflix.titus.common.util.guice.ProxyType;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.common.util.rx.ObservableExt;
import com.netflix.titus.master.MetricConstants;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.CompositeResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption;
import com.netflix.titus.master.service.management.ResourceConsumption.ConsumptionLevel;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupAllocationEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupRemovedEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.CapacityGroupUndefinedEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionEvents.ResourceConsumptionEvent;
import com.netflix.titus.master.service.management.ResourceConsumptionService;
import com.netflix.titus.master.service.management.ResourceConsumptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import static com.netflix.titus.common.util.CollectionsExt.copyAndRemove;

/**
 * Periodically checks for SLA violations, and a pre-configured interval.
 *
 * @TODO Improve this implementation by immediately reacting to state changes in the system.
 */
@Singleton
@ProxyConfiguration(types = ProxyType.ActiveGuard)
public class DefaultResourceConsumptionService implements ResourceConsumptionService {

    private static final Logger logger = LoggerFactory.getLogger(DefaultResourceConsumptionService.class);

    private static final String METRIC_CONSUMPTION = MetricConstants.METRIC_CAPACITY_MANAGEMENT + "consumption.";

    static final long UPDATE_INTERVAL_MS = 5000;

    private final Supplier<ConsumptionEvaluationResult> evaluator;
    private final Registry registry;
    private final Scheduler.Worker worker;

    private ResourceConsumptionServiceMetrics metrics;
    private Subscription subscription;

    private final PublishSubject<ResourceConsumptionEvent> eventsSubject = PublishSubject.create();

    private volatile ConsumptionEvaluationResult latestEvaluation;

    @Inject
    public DefaultResourceConsumptionService(ApplicationSlaManagementService applicationSlaManagementService,
                                             V3JobOperations v3JobOperations,
                                             Registry registry) {
        this(ResourceConsumptionEvaluator.newEvaluator(applicationSlaManagementService, v3JobOperations), registry, Schedulers.computation());
    }

    /* For testing */ DefaultResourceConsumptionService(Supplier<ConsumptionEvaluationResult> evaluator,
                                                        Registry registry,
                                                        Scheduler scheduler) {
        this.evaluator = evaluator;
        this.registry = registry;
        this.worker = scheduler.createWorker();
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        logger.info("Entering active mode");
        this.metrics = new ResourceConsumptionServiceMetrics(registry.createId(METRIC_CONSUMPTION), registry);
        this.subscription = worker.schedulePeriodically(this::updateInfo, 0, UPDATE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        return Observable.empty();
    }

    @PreDestroy
    public void shutdown() {
        if (subscription != null) {
            subscription.unsubscribe();
        }
        worker.unsubscribe();
        eventsSubject.onCompleted();
    }

    @Override
    public Optional<CompositeResourceConsumption> getSystemConsumption() {
        return latestEvaluation == null ? Optional.empty() : Optional.of(latestEvaluation.getSystemConsumption());
    }

    @Override
    public Observable<ResourceConsumptionEvent> resourceConsumptionEvents() {
        return eventsSubject.compose(ObservableExt.head(() -> {
            if (latestEvaluation == null) {
                return Collections.emptyList();
            }

            long now = worker.now();
            List<ResourceConsumptionEvent> allEvents = new ArrayList<>();

            // Capacity group consumptions
            Collection<ResourceConsumption> groupConsumptions = ResourceConsumptions.groupBy(
                    latestEvaluation.getSystemConsumption(), ConsumptionLevel.CapacityGroup
            ).values();
            groupConsumptions
                    .forEach(consumption ->
                            allEvents.add(new CapacityGroupAllocationEvent(consumption.getConsumerName(), now, (CompositeResourceConsumption) consumption))
                    );

            // Undefined capacity groups
            latestEvaluation.getUndefinedCapacityGroups()
                    .forEach(capacityGroup ->
                            allEvents.add(new CapacityGroupUndefinedEvent(capacityGroup, now))
                    );

            return allEvents;
        }));
    }

    private void updateInfo() {
        try {
            ConsumptionEvaluationResult evaluationResult = evaluator.get();
            metrics.update(evaluationResult);

            ConsumptionEvaluationResult oldEvaluation = latestEvaluation;
            this.latestEvaluation = evaluationResult;

            if (eventsSubject.hasObservers()) {
                notifyAboutRemovedCapacityGroups(oldEvaluation);
                notifyAboutUndefinedCapacityGroups(oldEvaluation);
                notifyAboutResourceConsumptionChange(oldEvaluation);
            }
        } catch (Exception e) {
            logger.warn("Resource consumption update failure", e);
        }
        logger.debug("Resource consumption update finished");
    }

    private void notifyAboutRemovedCapacityGroups(ConsumptionEvaluationResult oldEvaluation) {
        if (oldEvaluation != null) {
            Set<String> removed = copyAndRemove(oldEvaluation.getDefinedCapacityGroups(), latestEvaluation.getDefinedCapacityGroups());
            removed.forEach(category -> publishEvent(new CapacityGroupRemovedEvent(category, worker.now())));
        }
    }

    private void notifyAboutResourceConsumptionChange(ConsumptionEvaluationResult oldEvaluation) {
        Map<String, ResourceConsumption> newCapacityGroupConsumptions = ResourceConsumptions.groupBy(
                latestEvaluation.getSystemConsumption(), ConsumptionLevel.CapacityGroup
        );
        Map<String, ResourceConsumption> oldCapacityGroupConsumptions = oldEvaluation == null
                ? Collections.emptyMap()
                : ResourceConsumptions.groupBy(oldEvaluation.getSystemConsumption(), ConsumptionLevel.CapacityGroup);

        long now = worker.now();
        newCapacityGroupConsumptions.values().forEach(newConsumption -> {
            ResourceConsumption previous = oldCapacityGroupConsumptions.get(newConsumption.getConsumerName());
            if (previous == null || !previous.equals(newConsumption)) {
                publishEvent(new CapacityGroupAllocationEvent(newConsumption.getConsumerName(), now,
                        (CompositeResourceConsumption) newConsumption));
            }
        });
    }

    private void notifyAboutUndefinedCapacityGroups(ConsumptionEvaluationResult oldEvaluation) {
        long now = worker.now();
        Set<String> toNotify = oldEvaluation == null
                ? latestEvaluation.getUndefinedCapacityGroups()
                : copyAndRemove(latestEvaluation.getUndefinedCapacityGroups(), oldEvaluation.getUndefinedCapacityGroups());
        toNotify.forEach(capacityGroup -> publishEvent(new CapacityGroupUndefinedEvent(capacityGroup, now)));
    }

    private void publishEvent(ResourceConsumptionEvent event) {
        eventsSubject.onNext(event);
    }

    static class ConsumptionEvaluationResult {
        private final Set<String> definedCapacityGroups;
        private final Set<String> undefinedCapacityGroups;
        private final CompositeResourceConsumption systemConsumption;

        ConsumptionEvaluationResult(Set<String> definedCapacityGroups,
                                    Set<String> undefinedCapacityGroups,
                                    CompositeResourceConsumption systemConsumption) {
            this.definedCapacityGroups = definedCapacityGroups;
            this.undefinedCapacityGroups = undefinedCapacityGroups;
            this.systemConsumption = systemConsumption;
        }

        public Set<String> getDefinedCapacityGroups() {
            return definedCapacityGroups;
        }

        public Set<String> getUndefinedCapacityGroups() {
            return undefinedCapacityGroups;
        }

        public CompositeResourceConsumption getSystemConsumption() {
            return systemConsumption;
        }
    }
}