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
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.DisruptionBudget;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator;
import com.netflix.titus.testkit.model.job.JobGenerator;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.eviction.service.quota.job.ArchaiusEffectiveJobDisruptionBudgetResolver.PROPERTY_KEY;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.budget;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.selfManagedPolicy;
import static com.netflix.titus.testkit.model.eviction.DisruptionBudgetGenerator.unlimitedRate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArchaiusEffectiveJobDisruptionBudgetResolverTest {

    private static final Job<?> JOB_NOT_SELF_MANAGED = JobGenerator.oneBatchJob();

    private static final Job<?> JOB_SELF_MANAGED = JobGenerator.oneBatchJob().but(j ->
            j.toBuilder()
                    .withJobDescriptor(j.getJobDescriptor().toBuilder()
                            .withDisruptionBudget(budget(selfManagedPolicy(1_000), unlimitedRate(), Collections.emptyList()))
                            .build()
                    )
                    .build()
    );

    private static final DisruptionBudget FALLBACK_BUDGET_1 = budget(
            DisruptionBudgetGenerator.perTaskRelocationLimitPolicy(100),
            DisruptionBudgetGenerator.unlimitedRate(),
            Collections.emptyList()
    );

    private static final DisruptionBudget FALLBACK_BUDGET_2 = budget(
            DisruptionBudgetGenerator.perTaskRelocationLimitPolicy(50),
            DisruptionBudgetGenerator.unlimitedRate(),
            Collections.emptyList()
    );

    private final EntitySanitizer sanitizer = mock(EntitySanitizer.class);
    private final PropertyRepository repository = mock(PropertyRepository.class);
    private final StubbedProperty property = new StubbedProperty();

    @Before
    public void setUp() throws Exception {
        when(repository.get(PROPERTY_KEY, String.class)).thenReturn(property);
    }

    @Test
    public void testReturnsOriginalForNotSelfManagedJobs() throws Exception {
        property.set(toJson(FALLBACK_BUDGET_1));

        ArchaiusEffectiveJobDisruptionBudgetResolver resolver = new ArchaiusEffectiveJobDisruptionBudgetResolver(repository, sanitizer);
        assertThat(resolver.resolve(JOB_NOT_SELF_MANAGED)).isEqualTo(JOB_NOT_SELF_MANAGED.getJobDescriptor().getDisruptionBudget());
    }

    @Test
    public void testReturnsFallbackForSelfManagedJobs() throws Exception {
        property.set(toJson(FALLBACK_BUDGET_1));

        ArchaiusEffectiveJobDisruptionBudgetResolver resolver = new ArchaiusEffectiveJobDisruptionBudgetResolver(repository, sanitizer);
        assertThat(resolver.resolve(JOB_SELF_MANAGED)).isEqualTo(FALLBACK_BUDGET_1);
    }

    @Test
    public void testRefresh() throws Exception {
        property.set(toJson(FALLBACK_BUDGET_1));

        ArchaiusEffectiveJobDisruptionBudgetResolver resolver = new ArchaiusEffectiveJobDisruptionBudgetResolver(repository, sanitizer);
        assertThat(resolver.resolve(JOB_SELF_MANAGED)).isEqualTo(FALLBACK_BUDGET_1);

        property.set(toJson(FALLBACK_BUDGET_2));
        assertThat(resolver.resolve(JOB_SELF_MANAGED)).isEqualTo(FALLBACK_BUDGET_2);
    }

    private String toJson(DisruptionBudget disruptionBudget) throws JsonProcessingException {
        return ObjectMappers.storeMapper().writeValueAsString(disruptionBudget);
    }

    private class StubbedProperty implements com.netflix.archaius.api.Property<String> {

        private String value;
        private Consumer<String> consumer = newValue -> {
        };

        @Override
        public String get() {
            return value;
        }

        void set(String newValue) {
            this.value = newValue;
            consumer.accept(newValue);
        }

        @Override
        public String getKey() {
            return PROPERTY_KEY;
        }

        @Override
        public Subscription subscribe(Consumer<String> consumer) {
            this.consumer = consumer;
            return () -> {
            };
        }
    }
}