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

package com.netflix.titus.common.util.loadshedding.tokenbucket;

import java.time.Duration;
import java.util.List;

import com.netflix.archaius.config.DefaultSettableConfig;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.loadshedding.AdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerRequest;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class ArchaiusTokenBucketAdmissionControllerTest {

    private static final ScheduleDescriptor TEST_SCHEDULE_DESCRIPTOR = ArchaiusTokenBucketAdmissionController.SCHEDULE_DESCRIPTOR.toBuilder()
            .withInterval(Duration.ofMillis(1))
            .build();

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    private final DefaultSettableConfig config = new DefaultSettableConfig();

    private ArchaiusTokenBucketAdmissionController controller;

    private volatile AdmissionControllerDelegateMock currentDelegate;

    @Before
    public void setUp() {
        this.controller = new ArchaiusTokenBucketAdmissionController(
                new TokenBucketAdmissionConfigurationParser(config),
                configuration -> currentDelegate = new AdmissionControllerDelegateMock(configuration),
                TEST_SCHEDULE_DESCRIPTOR,
                titusRuntime
        );
    }

    @After
    public void tearDown() {
        Evaluators.acceptNotNull(controller, ArchaiusTokenBucketAdmissionController::close);
    }

    @Test
    public void testRefresh() {
        // Single configuration
        TokenBucketTestConfigurations.DEFAULT_SHARED_PROPERTIES.forEach(config::setProperty);
        await().until(() -> currentDelegate != null);

        AdmissionControllerDelegateMock firstDelegate = currentDelegate;
        assertThat(firstDelegate.configuration).hasSize(1);

        // Two configuration rules
        TokenBucketTestConfigurations.NOT_SHARED_PROPERTIES.forEach(config::setProperty);
        await().until(() -> currentDelegate != firstDelegate);

        AdmissionControllerDelegateMock secondDelegate = currentDelegate;
        assertThat(secondDelegate.configuration).hasSize(2);
    }

    private static class AdmissionControllerDelegateMock implements AdmissionController {

        private final List<TokenBucketConfiguration> configuration;

        private AdmissionControllerDelegateMock(List<TokenBucketConfiguration> configuration) {
            this.configuration = configuration;
        }

        @Override
        public AdmissionControllerResponse apply(AdmissionControllerRequest request) {
            return AdmissionControllerResponse.newBuilder().build();
        }
    }
}