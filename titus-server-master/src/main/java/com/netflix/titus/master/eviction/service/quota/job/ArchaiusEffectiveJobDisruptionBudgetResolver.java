/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.eviction.service.quota.job;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.AvailabilityPercentageLimitDisruptionBudgetPolicy;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudgetFunctions;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.PercentagePerHourDisruptionBudgetRate;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.SelfManagedDisruptionBudgetPolicy;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.common.model.validator.ValidationError;
import com.netflix.titus.common.util.StringExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.api.jobmanager.model.job.sanitizer.JobSanitizerBuilder.JOB_STRICT_SANITIZER;

@Singleton
public class ArchaiusEffectiveJobDisruptionBudgetResolver implements EffectiveJobDisruptionBudgetResolver {

    private static final Logger logger = LoggerFactory.getLogger(ArchaiusEffectiveJobDisruptionBudgetResolver.class);

    public static final String PROPERTY_KEY = "titusMaster.eviction.selfManagedFallbackJobDisruptionBudget";

    private static final DisruptionBudget DEFAULT_SELF_MANAGED_FALLBACK_JOB_DISRUPTION_BUDGET = DisruptionBudget.newBuilder()
            .withDisruptionBudgetPolicy(AvailabilityPercentageLimitDisruptionBudgetPolicy.newBuilder().withPercentageOfHealthyContainers(90).build())
            .withDisruptionBudgetRate(PercentagePerHourDisruptionBudgetRate.newBuilder().withMaxPercentageOfContainersRelocatedInHour(20).build())
            .withTimeWindows(Collections.emptyList())
            .build();

    private final PropertyRepository repository;
    private final EntitySanitizer sanitizer;
    private final AtomicReference<DisruptionBudget> disruptionBudgetRef;
    private final Property.Subscription subscription;

    @Inject
    public ArchaiusEffectiveJobDisruptionBudgetResolver(PropertyRepository repository,
                                                        @Named(JOB_STRICT_SANITIZER) EntitySanitizer sanitizer) {
        this.repository = repository;
        this.sanitizer = sanitizer;
        this.disruptionBudgetRef = new AtomicReference<>(refresh());
        this.subscription = repository.get(PROPERTY_KEY, String.class).subscribe(update -> disruptionBudgetRef.set(refresh()));
    }

    @PreDestroy
    public void shutdown() {
        subscription.unsubscribe();
    }

    @Override
    public DisruptionBudget resolve(Job<?> job) {
        return DisruptionBudgetFunctions.isSelfManaged(job) ? disruptionBudgetRef.get() : job.getJobDescriptor().getDisruptionBudget();
    }

    private DisruptionBudget refresh() {
        Property<String> property = repository.get(PROPERTY_KEY, String.class);
        if (property == null || StringExt.isEmpty(property.get())) {
            return DEFAULT_SELF_MANAGED_FALLBACK_JOB_DISRUPTION_BUDGET;
        }
        try {
            return parse(property.get());
        } catch (Exception e) {
            logger.warn("invalid SelfManaged fallback disruption budget configuration ({})", e.getMessage());
            return DEFAULT_SELF_MANAGED_FALLBACK_JOB_DISRUPTION_BUDGET;
        }
    }

    private DisruptionBudget parse(String newValue) throws Exception {
        DisruptionBudget budget = ObjectMappers.storeMapper().readValue(newValue, DisruptionBudget.class);

        Preconditions.checkArgument(
                !(budget.getDisruptionBudgetPolicy() instanceof SelfManagedDisruptionBudgetPolicy),
                "Self managed migration policy not allowed as a fallback"
        );

        DisruptionBudget sanitized = sanitizer.sanitize(budget).orElse(budget);
        Set<ValidationError> violations = sanitizer.validate(sanitized);

        Preconditions.checkState(violations.isEmpty(), "Invalid self managed fallback disruption budget: value=%s, violations=%s", sanitized, violations);

        return sanitized;
    }
}
