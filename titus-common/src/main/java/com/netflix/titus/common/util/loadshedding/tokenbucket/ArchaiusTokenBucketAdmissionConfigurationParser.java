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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.netflix.archaius.api.Config;
import com.netflix.titus.common.util.PropertiesExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example configuration layout:
 * default.order=100
 * default.sharedByCallers=true
 * default.callerPattern=.*
 * default.endpointPattern=.*
 * default.capacity=100
 * default.refillRateInSec=20
 */
public class ArchaiusTokenBucketAdmissionConfigurationParser implements Supplier<List<TokenBucketConfiguration>> {

    private static final Logger logger = LoggerFactory.getLogger(ArchaiusTokenBucketAdmissionConfigurationParser.class);

    private final Config config;
    private volatile List<TokenBucketConfiguration> bucketConfigurations = Collections.emptyList();

    public ArchaiusTokenBucketAdmissionConfigurationParser(Config config) {
        this.config = config;
    }

    public List<TokenBucketConfiguration> get() {
        List<TokenBucketConfiguration> currentConfiguration = parseToBucketConfiguration();
        currentConfiguration.sort(Comparator.comparingInt(TokenBucketConfiguration::getOrder));

        if (currentConfiguration.equals(bucketConfigurations)) {
            return bucketConfigurations;
        }
        return this.bucketConfigurations = currentConfiguration;
    }

    private List<TokenBucketConfiguration> parseToBucketConfiguration() {
        Map<String, String> allKeyValues = new HashMap<>();
        config.getKeys().forEachRemaining(key -> allKeyValues.put(key, config.getString(key)));

        Map<String, Map<String, String>> bucketProperties = PropertiesExt.groupByRootName(allKeyValues, 1);

        List<TokenBucketConfiguration> currentBucketConfigurations = new ArrayList<>();
        bucketProperties.forEach((name, bucketConfiguration) -> {
            try {
                currentBucketConfigurations.add(new TokenBucketConfiguration(
                        name,
                        Integer.parseInt(bucketConfiguration.get("order")),
                        Boolean.parseBoolean(bucketConfiguration.get("sharedByCallers")),
                        Preconditions.checkNotNull(bucketConfiguration.get("callerPattern"), "Caller pattern is null"),
                        Preconditions.checkNotNull(bucketConfiguration.get("endpointPattern"), "Endpoint pattern is null"),
                        Integer.parseInt(bucketConfiguration.get("capacity")),
                        Integer.parseInt(bucketConfiguration.get("refillRateInSec"))
                ));
            } catch (Exception e) {
                logger.warn("Invalid bucket configuration: {}={}", name, bucketConfiguration);
            }
        });
        return currentBucketConfigurations;
    }

}
