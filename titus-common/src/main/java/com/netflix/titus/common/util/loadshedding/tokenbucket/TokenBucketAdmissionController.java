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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.util.cache.Cache;
import com.netflix.titus.common.util.cache.Caches;
import com.netflix.titus.common.util.limiter.Limiters;
import com.netflix.titus.common.util.limiter.tokenbucket.TokenBucket;
import com.netflix.titus.common.util.loadshedding.AdmissionController;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerRequest;
import com.netflix.titus.common.util.loadshedding.AdmissionControllerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admission controller with multiple token buckets. A token bucket is selected by evaluating matching criteria in
 * declaration order until first match is found. The selected bucket is tried, and either success or failure is
 * returned to the caller.
 */
public class TokenBucketAdmissionController implements AdmissionController {

    private static final Logger logger = LoggerFactory.getLogger(TokenBucketAdmissionController.class);

    private static final AdmissionControllerResponse DEFAULT_OK = AdmissionControllerResponse.newBuilder()
            .withAllowed(true)
            .withReasonMessage("Rate limits not configured")
            .build();

    private static final int MAX_CACHE_SIZE = 10_000;
    private static final Duration CACHE_ITEM_TIMEOUT = Duration.ofSeconds(60);

    private final List<TokenBucketConfiguration> tokenBucketConfigurations;
    private final Cache<String, TokenBucketInstance> bucketsById;

    public TokenBucketAdmissionController(List<TokenBucketConfiguration> tokenBucketConfigurations,
                                          TitusRuntime titusRuntime) {
        this.tokenBucketConfigurations = tokenBucketConfigurations;

        this.bucketsById = Caches.instrumentedCacheWithMaxSize(
                MAX_CACHE_SIZE,
                CACHE_ITEM_TIMEOUT,
                "titus.tokenBucketAdmissionController.cache",
                titusRuntime.getRegistry()
        );
    }

    @Override
    public AdmissionControllerResponse apply(AdmissionControllerRequest request) {
        return findTokenBucket(request).map(this::consume).orElse(DEFAULT_OK);
    }

    private Optional<TokenBucketInstance> findTokenBucket(AdmissionControllerRequest request) {
        return tokenBucketConfigurations.stream()
                .filter(tokenBucketConfiguration -> matches(tokenBucketConfiguration, request))
                .findFirst()
                .map(tokenBucketConfiguration -> {
                    // If shared, use pattern name as key, otherwise use caller id
                    String id = tokenBucketConfiguration.isShared()
                            ? tokenBucketConfiguration.getCallerPatternString()
                            : request.getCallerId();

                    return bucketsById.get(id, i -> new TokenBucketInstance(tokenBucketConfiguration));
                });
    }

    private boolean matches(TokenBucketConfiguration configuration, AdmissionControllerRequest request) {
        try {
            if (!configuration.getCallerPattern().matcher(request.getCallerId()).matches()) {
                return false;
            }
            if (!configuration.getEndpointPattern().matcher(request.getEndpointName()).matches()) {
                return false;
            }
            return true;
        } catch (Exception e) {
            logger.warn("Unexpected error", e);
            return false;
        }
    }

    private AdmissionControllerResponse consume(TokenBucketInstance tokenBucketInstance) {
        TokenBucket tokenBucket = tokenBucketInstance.getTokenBucket();
        if (tokenBucket.tryTake()) {
            return AdmissionControllerResponse.newBuilder()
                    .withAllowed(true)
                    .withReasonMessage(String.format(
                            "Consumed token of: bucketName=%s, remainingTokens=%s",
                            tokenBucketInstance.getConfiguration().getName(),
                            tokenBucket.getNumberOfTokens()
                    )).build();
        }
        return AdmissionControllerResponse.newBuilder()
                .withAllowed(false)
                .withReasonMessage(String.format(
                        "No more tokens: bucketName=%s, remainingTokens=%s",
                        tokenBucketInstance.getConfiguration().getName(),
                        tokenBucket.getNumberOfTokens()
                )).build();
    }

    private static class TokenBucketInstance {

        private final TokenBucketConfiguration configuration;
        private final TokenBucket tokenBucket;

        private TokenBucketInstance(TokenBucketConfiguration configuration) {
            this.configuration = configuration;
            this.tokenBucket = Limiters.createFixedIntervalTokenBucket(
                    configuration.getName(),
                    configuration.getCapacity(),
                    configuration.getCapacity(),
                    configuration.getRefillRateInSec(),
                    1,
                    TimeUnit.SECONDS
            );
        }

        public TokenBucketConfiguration getConfiguration() {
            return configuration;
        }

        public TokenBucket getTokenBucket() {
            return tokenBucket;
        }
    }
}
