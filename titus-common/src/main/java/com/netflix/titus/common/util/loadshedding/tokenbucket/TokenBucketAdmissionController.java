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
import com.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Admission controller with multiple token buckets. A token bucket is selected by evaluating matching criteria in
 * declaration order until first match is found. The selected bucket is tried, and either success or failure is
 * returned to the caller.
 * <p/>
 * <h1>Example: shared caller bucket</h1>
 * slowMethods.sharedByCallers=true<br/>
 * slowMethods.callerPattern=.*<br/>
 * slowMethods.endpointPattern=create.*<br/>
 * ...</br>
 * <p/>
 * fastMethods.sharedByCallers=true<br/>
 * fastMethods.callerPattern=.*<br/>
 * fastMethods.endpointPattern=get.*<br/>
 * ...</br>
 * <p/>
 * Caller Alice and Bob making a call to createJob method will share a single bucket with id (.*, create.*), and
 * a single bucket for methods getJob/getTask/etc with id (.*, get.*).
 * <p/>
 * <h1>Example: per caller bucket</h1>
 * slowMethods.sharedByCallers=false<br/>
 * slowMethods.callerPattern=.*<br/>
 * slowMethods.endpointPattern=create.*<br/>
 * ...</br>
 * <p/>
 * fastMethods.sharedByCallers=false<br/>
 * fastMethods.callerPattern=.*<br/>
 * fastMethods.endpointPattern=get.*<br/>
 * ...</br>
 * <p/>
 * Caller Alice and Bob making a call to createJob method will have own buckets with ids (Alice, create.*) and
 * (Bob, create.*). Similarly, for methods getJob/getTask/etc there will be two buckets (Alice, get.*) and
 * (Bob, get.*) respectively.
 */
public class TokenBucketAdmissionController implements AdmissionController {

    private static final Logger logger = LoggerFactory.getLogger(TokenBucketAdmissionController.class);

    private static final AdmissionControllerResponse DEFAULT_OK = AdmissionControllerResponse.newBuilder()
            .withAllowed(true)
            .withReasonMessage("Rate limits not configured")
            .build();

    private static final int MAX_CACHE_SIZE = 10_000;
    private static final Duration CACHE_ITEM_TIMEOUT = Duration.ofSeconds(600);

    private final List<TokenBucketConfiguration> tokenBucketConfigurations;

    /**
     * Bucket id consists of a caller id (or caller pattern), and endpoint pattern.
     */
    private final Cache<Pair<String, String>, TokenBucketInstance> bucketsById;

    /**
     * Maps requests to its assigned bucket ids. We cannot map to the {@link TokenBucketInstance} directly, as its
     * lifecycle is managed by {@link #bucketsById} cache, and we would not know when it was recreated.
     */
    private final Cache<AdmissionControllerRequest, Pair<String, String>> requestToBucketIdCache;

    public TokenBucketAdmissionController(List<TokenBucketConfiguration> tokenBucketConfigurations,
                                          TitusRuntime titusRuntime) {
        this.tokenBucketConfigurations = tokenBucketConfigurations;

        this.bucketsById = Caches.instrumentedCacheWithMaxSize(
                MAX_CACHE_SIZE,
                CACHE_ITEM_TIMEOUT,
                "titus.tokenBucketAdmissionController.cache",
                titusRuntime.getRegistry()
        );
        this.requestToBucketIdCache = Caches.instrumentedCacheWithMaxSize(
                MAX_CACHE_SIZE,
                CACHE_ITEM_TIMEOUT,
                "titus.tokenBucketAdmissionController.requestCache",
                titusRuntime.getRegistry()
        );
    }

    @Override
    public AdmissionControllerResponse apply(AdmissionControllerRequest request) {
        return findTokenBucket(request).map(this::consume).orElse(DEFAULT_OK);
    }

    private Optional<TokenBucketInstance> findTokenBucket(AdmissionControllerRequest request) {
        Pair<String, String> bucketId = requestToBucketIdCache.getIfPresent(request);
        if (bucketId != null) {
            TokenBucketInstance instance = bucketsById.getIfPresent(bucketId);
            if (instance != null) {
                return Optional.of(instance);
            }
        }

        TokenBucketConfiguration tokenBucketConfiguration = tokenBucketConfigurations.stream()
                .filter(configuration -> matches(configuration, request))
                .findFirst()
                .orElse(null);

        if (tokenBucketConfiguration == null) {
            return Optional.empty();
        }

        // If shared, use pattern name as key, otherwise use caller id
        String effectiveCallerId = tokenBucketConfiguration.isSharedByCallers()
                ? tokenBucketConfiguration.getCallerPatternString()
                : request.getCallerId();

        if (bucketId == null) {
            bucketId = Pair.of(effectiveCallerId, tokenBucketConfiguration.getEndpointPatternString());
            requestToBucketIdCache.put(request, bucketId);
        }

        return Optional.ofNullable(bucketsById.get(bucketId, i -> new TokenBucketInstance(effectiveCallerId, tokenBucketConfiguration)));
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
        AdmissionControllerResponse.Builder builder = AdmissionControllerResponse.newBuilder();

        TokenBucket tokenBucket = tokenBucketInstance.getTokenBucket();
        if (tokenBucket.tryTake()) {
            builder.withAllowed(true)
                    .withReasonMessage(String.format(
                            "Consumed token of: bucketName=%s, remainingTokens=%s",
                            tokenBucketInstance.getConfiguration().getName(),
                            tokenBucket.getNumberOfTokens()
                    ));
        } else {
            builder.withAllowed(false)
                    .withReasonMessage(String.format(
                            "No more tokens: bucketName=%s, remainingTokens=%s",
                            tokenBucketInstance.getConfiguration().getName(),
                            tokenBucket.getNumberOfTokens())
                    );
        }

        return builder.withDecisionPoint(TokenBucketAdmissionController.class.getSimpleName())
                .withEquivalenceGroup(tokenBucketInstance.getId())
                .build();
    }

    private static class TokenBucketInstance {

        private final TokenBucketConfiguration configuration;
        private final TokenBucket tokenBucket;
        private final String id;

        private TokenBucketInstance(String effectiveCallerId, TokenBucketConfiguration configuration) {
            this.configuration = configuration;
            this.tokenBucket = Limiters.createFixedIntervalTokenBucket(
                    configuration.getName(),
                    configuration.getCapacity(),
                    configuration.getCapacity(),
                    configuration.getRefillRateInSec(),
                    1,
                    TimeUnit.SECONDS
            );

            this.id = String.format("%s/%s", configuration.getName(), effectiveCallerId);
        }

        public TokenBucketConfiguration getConfiguration() {
            return configuration;
        }

        public String getId() {
            return id;
        }

        public TokenBucket getTokenBucket() {
            return tokenBucket;
        }
    }
}
