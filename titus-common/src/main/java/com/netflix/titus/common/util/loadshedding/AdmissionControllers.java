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

package com.netflix.titus.common.util.loadshedding;

import java.util.function.Supplier;

import com.netflix.archaius.api.Config;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.loadshedding.backoff.NoOpAdmissionBackoffStrategy;
import com.netflix.titus.common.util.loadshedding.backoff.SimpleAdmissionBackoffStrategy;
import com.netflix.titus.common.util.loadshedding.backoff.SimpleAdmissionBackoffStrategyConfiguration;
import com.netflix.titus.common.util.loadshedding.tokenbucket.ArchaiusTokenBucketAdmissionConfigurationParser;
import com.netflix.titus.common.util.loadshedding.tokenbucket.ConfigurableTokenBucketAdmissionController;

/**
 * {@link AdmissionController} factory.
 */
public final class AdmissionControllers {

    public static AdmissionBackoffStrategy noBackoff() {
        return NoOpAdmissionBackoffStrategy.getInstance();
    }

    public static AdmissionBackoffStrategy simpleBackoff(String id,
                                                         SimpleAdmissionBackoffStrategyConfiguration configuration,
                                                         TitusRuntime titusRuntime) {
        return new SimpleAdmissionBackoffStrategy(id, configuration, titusRuntime);
    }

    public static AdmissionController circuitBreaker(AdmissionController delegate, Supplier<Boolean> condition) {
        return new CircuitBreakerAdmissionController(delegate, condition);
    }

    public static AdaptiveAdmissionController circuitBreaker(AdaptiveAdmissionController delegate, Supplier<Boolean> condition) {
        return new CircuitBreakerAdaptiveAdmissionController(delegate, condition);
    }

    public static AdaptiveAdmissionController fixed(AdmissionControllerResponse response) {
        return new FixedResponseAdmissionController(response);
    }

    public static AdmissionController spectator(AdmissionController delegate, TitusRuntime titusRuntime) {
        return new SpectatorAdmissionController(delegate, titusRuntime);
    }

    public static AdaptiveAdmissionController spectator(AdaptiveAdmissionController delegate, TitusRuntime titusRuntime) {
        return new SpectatorAdaptiveAdmissionController(delegate, titusRuntime);
    }

    public static AdmissionController tokenBucketsFromArchaius(Config config, boolean includeDetailsInResponse, TitusRuntime titusRuntime) {
        return tokenBucketsFromArchaius(config, noBackoff(), includeDetailsInResponse, titusRuntime);
    }

    public static AdaptiveAdmissionController tokenBucketsFromArchaius(Config config,
                                                                       AdmissionBackoffStrategy backoffStrategy,
                                                                       boolean includeDetailsInResponse,
                                                                       TitusRuntime titusRuntime) {
        return new ConfigurableTokenBucketAdmissionController(
                new ArchaiusTokenBucketAdmissionConfigurationParser(config),
                backoffStrategy,
                includeDetailsInResponse,
                titusRuntime
        );
    }
}
