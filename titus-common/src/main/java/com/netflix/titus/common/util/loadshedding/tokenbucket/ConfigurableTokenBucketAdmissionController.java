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
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.netflix.titus.common.framework.scheduler.ExecutionContext;
import com.netflix.titus.common.framework.scheduler.ScheduleReference;
import com.netflix.titus.common.framework.scheduler.model.ScheduleDescriptor;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.loadshedding.AdaptiveAdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionBackoffStrategy;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerRequest;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerResponse;
import com.netflix.titus.common.util.loadshedding.FixedResponseAdmissionController;
import com.netflix.titus.common.util.retry.Retryers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around {@link TokenBucketAdmissionController} which is created from the dynamically loaded configuration.
 * When configuration changes, the current {@link TokenBucketAdmissionController} instance is discarded, a new one
 * is created. As a result token buckets state is reset, which is fine as long as changes do not happen too
 * frequently.
 */
public class ConfigurableTokenBucketAdmissionController implements AdaptiveAdmissionController, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ConfigurableTokenBucketAdmissionController.class);

    @VisibleForTesting
    static final ScheduleDescriptor SCHEDULE_DESCRIPTOR = ScheduleDescriptor.newBuilder()
            .withName(ConfigurableTokenBucketAdmissionController.class.getSimpleName())
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
            .withDecisionPoint(ConfigurableTokenBucketAdmissionController.class.getSimpleName())
            .withEquivalenceGroup("all")
            .build();

    private final Supplier<List<TokenBucketConfiguration>> configurationSupplier;
    private final Function<List<TokenBucketConfiguration>, AdaptiveAdmissionController> delegateFactory;

    private final ScheduleReference ref;

    private volatile List<TokenBucketConfiguration> activeConfiguration = Collections.emptyList();
    private volatile AdaptiveAdmissionController delegate = new FixedResponseAdmissionController(ALL_ALLOWED);

    public ConfigurableTokenBucketAdmissionController(Supplier<List<TokenBucketConfiguration>> configurationSupplier,
                                                      AdmissionBackoffStrategy admissionBackoffStrategy,
                                                      TitusRuntime titusRuntime) {
        this(configurationSupplier,
                tokenBucketConfigurations -> new TokenBucketAdmissionController(tokenBucketConfigurations,
                        admissionBackoffStrategy, titusRuntime),
                SCHEDULE_DESCRIPTOR,
                titusRuntime
        );
    }

    @VisibleForTesting
    ConfigurableTokenBucketAdmissionController(Supplier<List<TokenBucketConfiguration>> configurationSupplier,
                                               Function<List<TokenBucketConfiguration>, AdaptiveAdmissionController> delegateFactory,
                                               ScheduleDescriptor scheduleDescriptor,
                                               TitusRuntime titusRuntime) {
        this.configurationSupplier = configurationSupplier;
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

    @Override
    public void onSuccess(long elapsedMs) {
        delegate.onSuccess(elapsedMs);
    }

    @Override
    public void onError(long elapsedMs, ErrorKind errorKind, Throwable cause) {
        delegate.onError(elapsedMs, errorKind, cause);
    }

    private void reload(ExecutionContext context) {
        List<TokenBucketConfiguration> current = configurationSupplier.get();
        if (!current.equals(activeConfiguration)) {
            this.delegate = delegateFactory.apply(current);
            this.activeConfiguration = current;

            logger.info("Reloaded configuration: {}", current);
        }
    }
}
