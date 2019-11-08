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

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.loadshedding.AdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerRequest;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerResponse;
import com.netflix.titus.common.util.retry.Retryers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around {@link TokenBucketAdmissionController} which dynamically loads configuration from Archaius.
 * When configuration changes, the current {@link TokenBucketAdmissionController} instance is discarded, a new one
 * is created. As a result token buckets state is reset, which is fine as long as changes do not happen too
 * frequently.
 */
public class ArchaiusTokenBucketAdmissionController implements AdmissionController, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ArchaiusTokenBucketAdmissionController.class);

    @VisibleForTesting
    static final ScheduleDescriptor SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(ArchaiusTokenBucketAdmissionController.class.getSimpleName())
            .withDescription("Configuration re-loader")
            .withInitialDelay(Duration.ZERO)
            .withInterval(Duration.ofSeconds(1))
            .withTimeout(Duration.ofSeconds(1))
            .withRetryerSupplier(Retryers::never)
            .withOnErrorHandler((action, error) -> {
                logger.warn("Cannot load configuration: {}", error.getMessage());
                logger.debug(error.getMessage(), error);
            })
            .build();

    private static final AdmissionControllerResponse ALL_ALLOWED = AdmissionControllerResponse.newBuilder()
            .withAllowed(true)
            .withReasonMessage("Admission controller configuration not found")
            .build();

    private final TokenBucketAdmissionConfigurationParser configurationParser;
    private final Function<List<TokenBucketConfiguration>, AdmissionController> delegateFactory;

    private final ScheduleReference ref;

    private volatile List<TokenBucketConfiguration> activeConfiguration = Collections.emptyList();
    private volatile AdmissionController delegate = any -> ALL_ALLOWED;

    public ArchaiusTokenBucketAdmissionController(TokenBucketAdmissionConfigurationParser configurationParser,
                                                  TitusRuntime titusRuntime) {
        this(configurationParser,
                tokenBucketConfigurations -> new TokenBucketAdmissionController(tokenBucketConfigurations, titusRuntime),
                SCHEDULE_DESCRIPTOR,
                titusRuntime
        );
    }

    @VisibleForTesting
    ArchaiusTokenBucketAdmissionController(TokenBucketAdmissionConfigurationParser configurationParser,
                                           Function<List<TokenBucketConfiguration>, AdmissionController> delegateFactory,
                                           ScheduleDescriptor scheduleDescriptor,
                                           TitusRuntime titusRuntime) {
        this.configurationParser = configurationParser;
        this.delegateFactory = delegateFactory;
        this.ref = titusRuntime.getLocalScheduler().schedule(scheduleDescriptor, this::reload, false);
    }


    @Override
    public void close() {
        ref.cancel();
    }

    @Override
    public AdmissionControllerResponse apply(AdmissionControllerRequest request) {
        return delegate.apply(request);
    }

    private void reload(ExecutionContext context) {
        List<TokenBucketConfiguration> current = configurationParser.parse();
        if (current != activeConfiguration) {
            this.delegate = delegateFactory.apply(current);
            this.activeConfiguration = current;

            logger.info("Reloaded configuration: {}", current);
        }
    }
}
