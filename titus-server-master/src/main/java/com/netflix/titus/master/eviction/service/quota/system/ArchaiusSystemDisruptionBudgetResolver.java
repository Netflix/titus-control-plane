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

package com.netflix.titus.master.eviction.service.quota.system;

import java.util.Collections;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.Day;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindow;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.api.model.FixedIntervalTokenBucketRefillPolicy;
import com.netflix.titus.api.model.TokenBucketPolicy;
import com.netflix.titus.api.model.reference.Reference;
import com.netflix.titus.common.util.StringExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ReplayProcessor;

import static com.netflix.titus.api.eviction.model.SystemDisruptionBudget.newBasicSystemDisruptionBudget;

@Singleton
public class ArchaiusSystemDisruptionBudgetResolver implements SystemDisruptionBudgetResolver {

    private static final Logger logger = LoggerFactory.getLogger(ArchaiusSystemDisruptionBudgetResolver.class);

    public static final String PROPERTY_KEY = "titusMaster.eviction.systemDisruptionBudget";

    private static final SystemDisruptionBudget DEFAULT_SYSTEM_DISRUPTION_BUDGET = newBasicSystemDisruptionBudget(
            50,
            500,
            TimeWindow.newBuilder()
                    .withDays(Day.weekdays())
                    .withwithHourlyTimeWindows(8, 16)
                    .withTimeZone("PST")
                    .build()
    );

    private final Property.Subscription subscription;

    private final ReplayProcessor<SystemDisruptionBudget> budgetEmitter;

    @Inject
    public ArchaiusSystemDisruptionBudgetResolver(PropertyRepository repository) {
        this.budgetEmitter = ReplayProcessor.cacheLastOrDefault(initialBudget(repository));
        this.subscription = repository.get(PROPERTY_KEY, String.class).subscribe(this::processUpdate);
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }

    @Override
    public Flux<SystemDisruptionBudget> resolve() {
        return budgetEmitter;
    }

    private SystemDisruptionBudget initialBudget(PropertyRepository repository) {
        Property<String> property = repository.get(PROPERTY_KEY, String.class);
        if (property == null || StringExt.isEmpty(property.get())) {
            return DEFAULT_SYSTEM_DISRUPTION_BUDGET;
        }
        try {
            return parse(property.get());
        } catch (Exception e) {
            throw EvictionException.badConfiguration("invalid system disruption budget configuration (%s)", e.getMessage());
        }
    }

    private void processUpdate(String newValue) {
        try {
            budgetEmitter.onNext(parse(newValue));
        } catch (Exception e) {
            logger.warn("Invalid system disruption budget configured: newValue='{}', error={}", newValue, e.getMessage());
        }
    }

    private SystemDisruptionBudget parse(String newValue) throws Exception {
        SystemDisruptionBudgetDescriptor descriptor = ObjectMappers.storeMapper().readValue(newValue, SystemDisruptionBudgetDescriptor.class);

        if (descriptor.getRefillRatePerSecond() < 0) {
            throw EvictionException.badConfiguration("system disruption budget refills < 0");
        }
        if (descriptor.getCapacity() < 0) {
            throw EvictionException.badConfiguration("system disruption budget capacity < 0");
        }

        return toDisruptionBudget(descriptor);
    }

    @VisibleForTesting
    static SystemDisruptionBudget toDisruptionBudget(SystemDisruptionBudgetDescriptor descriptor) {
        return SystemDisruptionBudget.newBuilder()
                .withReference(Reference.system())
                .withTokenBucketDescriptor(TokenBucketPolicy.newBuilder()
                        .withCapacity(descriptor.getCapacity())
                        .withInitialNumberOfTokens(0)
                        .withRefillPolicy(FixedIntervalTokenBucketRefillPolicy.newBuilder()
                                .withIntervalMs(1_000)
                                .withNumberOfTokensPerInterval(descriptor.getRefillRatePerSecond())
                                .build()
                        )
                        .build()
                )
                .withTimeWindows(descriptor.getTimeWindows() != null ? descriptor.getTimeWindows() : Collections.emptyList())
                .build();
    }
}
