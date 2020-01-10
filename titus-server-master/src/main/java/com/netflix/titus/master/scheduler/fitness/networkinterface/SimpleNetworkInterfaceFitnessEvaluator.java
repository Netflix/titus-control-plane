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

package com.netflix.titus.master.scheduler.fitness.networkinterface;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import com.netflix.titus.common.util.MathExt;
import com.netflix.titus.common.util.time.Clock;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import com.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;
import com.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheNetworkInterface;

import static com.netflix.titus.master.scheduler.fitness.FitnessCalculatorFunctions.areSecurityGroupsEqual;

/**
 * Prefers to choose network interfaces in the following order:
 * <ul>
 * <li>bin pack a network interface with running tasks with the same security groups</li>
 * <li>already configured with the correct security groups</li>
 * <li>not in the cache</li>
 * <li>last used as close to or greater than {@link SchedulerConfiguration#getPreferredNetworkInterfaceDelayMs()}</li>
 * </ul>
 * Note that the scores are scaled to reduce the variance as this score is aggregated with the other fenzo scores.
 */
public class SimpleNetworkInterfaceFitnessEvaluator implements PreferentialNamedConsumableResourceEvaluator {

    // Note that evaluateIdle scores must always be less than evaluate scores or consumable resources will not be re-used.
    private static final double MIN_BIN_PACKED_SCORE = 0.11;
    private static final double MAX_BIN_PACKED_SCORE = 0.20;
    private static final double ALREADY_CONFIGURED_SCORE = 0.100;
    private static final double UNKNOWN_SCORE = 0.099;
    private static final double MIN_DELAYED_SCORE = 0.090;
    private static final double MAX_DELAYED_SCORE = 0.098;
    private static final String SECURITY_GROUP_DELIMITER = ":";

    private final AgentResourceCache cache;
    private final SchedulerConfiguration configuration;
    private final Clock clock;

    public SimpleNetworkInterfaceFitnessEvaluator(AgentResourceCache cache,
                                                  SchedulerConfiguration configuration,
                                                  Clock clock) {
        this.cache = cache;
        this.configuration = configuration;
        this.clock = clock;
    }

    @Override
    public double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit) {
        AgentResourceCacheNetworkInterface networkInterface = null;
        Optional<AgentResourceCacheInstance> cacheInstanceOpt = cache.get(hostname);

        if (cacheInstanceOpt.isPresent()) {
            networkInterface = cacheInstanceOpt.get().getNetworkInterface(index);
        }

        if (networkInterface == null) {
            return UNKNOWN_SCORE;
        }

        List<String> securityGroups = Arrays.asList(resourceName.split(SECURITY_GROUP_DELIMITER));

        if (areSecurityGroupsEqual(securityGroups, networkInterface.getSecurityGroupIds())) {
            return ALREADY_CONFIGURED_SCORE;
        }

        long delayMs = configuration.getPreferredNetworkInterfaceDelayMs();
        long lastUpdatedTimestamp = networkInterface.getTimestamp();
        long timeDifference = MathExt.between(clock.wallTime() - lastUpdatedTimestamp, 0, delayMs);
        return MathExt.scale(timeDifference, 0.0, delayMs, MIN_DELAYED_SCORE, MAX_DELAYED_SCORE);
    }

    @Override
    public double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        // Calculate the bin packing score between 0.0 and 1.0 and then scale it such that it is between MIN_BIN_PACKED_SCORE and MAX_BIN_PACKED_SCORE
        double score = (subResourcesUsed + subResourcesNeeded + 1.0) / (subResourcesLimit + 1);
        return MathExt.scale(score, 0.0, 1.0, MIN_BIN_PACKED_SCORE, MAX_BIN_PACKED_SCORE);
    }
}
