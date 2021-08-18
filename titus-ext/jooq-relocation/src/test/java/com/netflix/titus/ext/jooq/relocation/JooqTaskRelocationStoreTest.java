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

package com.netflix.titus.ext.jooq.relocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.netflix.titus.api.relocation.model.TaskRelocationPlan;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.ext.jooq.JooqContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        properties = {
                "spring.application.name=test",
                "titus.ext.jooq.relocation.inMemoryDb=true"
        },
        classes = {
                JooqRelocationContextComponent.class,
                JooqTaskRelocationStoreTest.class,
        }
)
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
public class JooqTaskRelocationStoreTest {

    @Bean
    public TitusRuntime getTitusRuntime() {
        return TitusRuntimes.internal();
    }

    @Autowired
    public JooqContext jooqContext;

    private JooqTaskRelocationStore store;

    @Before
    public void setUp() {
        this.store = newStore();
    }

    @After
    public void tearDown() {
        StepVerifier.create(store.clearStore()).verifyComplete();
    }

    @Test
    public void testRelocationPlanStoreCrud() {
        List<TaskRelocationPlan> plans = newRelocationPlans(1);
        TaskRelocationPlan plan = plans.get(0);

        // Create
        Map<String, Optional<Throwable>> result = store.createOrUpdateTaskRelocationPlans(plans).block();
        assertThat(result).hasSize(1);
        assertThat(result.get(plan.getTaskId())).isEmpty();

        // Reboot (to force reload from the database).
        this.store = newStore();

        // Read
        assertThat(store.getAllTaskRelocationPlans().block()).hasSize(1);
        assertThat(store.getAllTaskRelocationPlans().block().get(plan.getTaskId())).isEqualTo(plan);

        // Update
        TaskRelocationPlan updatedPlan = plan.toBuilder().withReasonMessage("Updated...").build();
        Map<String, Optional<Throwable>> updatedPlanResult = store.createOrUpdateTaskRelocationPlans(Collections.singletonList(updatedPlan)).block();
        assertThat(updatedPlanResult).hasSize(1);
        assertThat(store.getAllTaskRelocationPlans().block().get(plan.getTaskId())).isEqualTo(updatedPlan);

        // Delete
        Map<String, Optional<Throwable>> deleteResult = store.removeTaskRelocationPlans(Collections.singleton(plan.getTaskId())).block();
        assertThat(deleteResult).hasSize(1);

        // Reboot
        this.store = newStore();
        assertThat(store.getAllTaskRelocationPlans().block()).hasSize(0);
    }

    @Test
    public void testStoringLargeAmountOfPlans() {
        List<TaskRelocationPlan> plans = newRelocationPlans(10_000);

        // Create
        Map<String, Optional<Throwable>> result = store.createOrUpdateTaskRelocationPlans(plans).block();
        assertThat(result).hasSize(plans.size());
        long failures = result.values().stream().filter(Optional::isPresent).count();
        assertThat(failures).isZero();

        // Reboot
        this.store = newStore();
        assertThat(store.getAllTaskRelocationPlans().block()).hasSize(10_000);
    }

    private JooqTaskRelocationStore newStore() {
        JooqTaskRelocationStore store = new JooqTaskRelocationStore(jooqContext.getDslContext());
        store.activate();
        return store;
    }

    private List<TaskRelocationPlan> newRelocationPlans(int count) {
        List<TaskRelocationPlan> plans = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            plans.add(TaskRelocationPlan.newBuilder()
                    .withTaskId("task" + i)
                    .withReason(TaskRelocationPlan.TaskRelocationReason.TaskMigration)
                    .withReasonMessage("Test...")
                    .withRelocationTime(123)
                    .build()
            );
        }
        return plans;
    }
}