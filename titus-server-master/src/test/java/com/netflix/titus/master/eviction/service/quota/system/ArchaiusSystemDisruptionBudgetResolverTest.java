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
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.archaius.api.PropertyRepository;
import com.netflix.titus.api.eviction.model.SystemDisruptionBudget;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.eviction.service.EvictionException.ErrorCode;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.Day;
import com.netflix.titus.api.jobmanager.model.job.disruptionbudget.TimeWindow;
import com.netflix.titus.api.json.ObjectMappers;
import com.netflix.titus.testkit.rx.TitusRxSubscriber;
import org.junit.Before;
import org.junit.Test;

import static com.netflix.titus.master.eviction.service.quota.system.ArchaiusSystemDisruptionBudgetResolver.PROPERTY_KEY;
import static com.netflix.titus.master.eviction.service.quota.system.ArchaiusSystemDisruptionBudgetResolver.toDisruptionBudget;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArchaiusSystemDisruptionBudgetResolverTest {

    private static final SystemDisruptionBudgetDescriptor DESCRIPTOR_1_1 = new SystemDisruptionBudgetDescriptor(
            1,
            1,
            Collections.singletonList(TimeWindow.newBuilder()
                    .withDays(Day.weekdays())
                    .withwithHourlyTimeWindows(8, 16)
                    .withTimeZone("PST")
                    .build()
            )
    );
    private static final SystemDisruptionBudgetDescriptor DESCRIPTOR_2_2 = new SystemDisruptionBudgetDescriptor(2, 2, Collections.emptyList());

    private final PropertyRepository repository = mock(PropertyRepository.class);
    private final StubbedProperty property = new StubbedProperty();

    private final TitusRxSubscriber<SystemDisruptionBudget> testSubscriber = new TitusRxSubscriber<>();

    @Before
    public void setUp() throws Exception {
        when(repository.get(PROPERTY_KEY, String.class)).thenReturn(property);
    }

    @Test
    public void testResolve() throws Exception {
        property.set(toJson(DESCRIPTOR_1_1));

        ArchaiusSystemDisruptionBudgetResolver resolver = new ArchaiusSystemDisruptionBudgetResolver(repository);
        resolver.resolve().subscribe(testSubscriber);

        // Should return the initial value.
        assertThat(testSubscriber.takeNext()).isEqualTo(toDisruptionBudget(DESCRIPTOR_1_1));

        // Now refresh
        property.set(toJson(DESCRIPTOR_2_2));
        assertThat(testSubscriber.takeNext()).isEqualTo(toDisruptionBudget(DESCRIPTOR_2_2));
    }

    @Test
    public void testExceptionOnCreateIsPropagated() {
        property.set("bad data");
        try {
            new ArchaiusSystemDisruptionBudgetResolver(repository);
            fail("Exception expected");
        } catch (EvictionException e) {
            assertThat(e.getErrorCode()).isEqualTo(ErrorCode.BadConfiguration);
        }
    }

    @Test
    public void testLaterExceptionsAreReportedAndSwallowed() throws Exception {
        property.set(toJson(DESCRIPTOR_1_1));

        ArchaiusSystemDisruptionBudgetResolver resolver = new ArchaiusSystemDisruptionBudgetResolver(repository);
        resolver.resolve().subscribe(testSubscriber);

        // Should return the initial value.
        assertThat(testSubscriber.takeNext()).isEqualTo(toDisruptionBudget(DESCRIPTOR_1_1));

        // Now set bad data
        property.set("bad data");
        assertThat(testSubscriber.takeNext()).isNull();

        // Now good again
        property.set(toJson(DESCRIPTOR_2_2));
        assertThat(testSubscriber.takeNext()).isEqualTo(toDisruptionBudget(DESCRIPTOR_2_2));
    }

    private String toJson(SystemDisruptionBudgetDescriptor systemDisruptionBudgetDescriptor) throws JsonProcessingException {
        return ObjectMappers.storeMapper().writeValueAsString(systemDisruptionBudgetDescriptor);
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