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

package io.netflix.titus.master.scheduler.fitness.networkinterface;

import java.util.Optional;

import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import io.netflix.titus.common.util.MathExt;
import io.netflix.titus.common.util.time.Clock;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheInstance;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCacheNetworkInterface;

/**
 * Prefers to choose network interfaces in the following order:
 * <ul>
 * <li>already configured with the correct security groups</li>
 * <li>not in the cache</li>
 * <li>last used as close to or greater than {@link SchedulerConfiguration#getPreferredNetworkInterfaceDelayMs()}</li>
 * </ul>
 */
public class SimpleNetworkInterfaceFitnessEvaluator implements PreferentialNamedConsumableResourceEvaluator {

    private static final double ALREADY_CONFIGURED_SCORE = 1.0;
    private static final double UNKNOWN_SCORE = 0.99;
    private static final double MIN_DELAYED_SCORE = 0.90;
    private static final double MAX_DELAYED_SCORE = 0.98;

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

        if (resourceName.equals(networkInterface.getJoinedSecurityGroupIds())) {
            return ALREADY_CONFIGURED_SCORE;
        }

        long delayMs = configuration.getPreferredNetworkInterfaceDelayMs();
        long lastUpdatedTimestamp = networkInterface.getTimestamp();
        long timeDifference = Math.min(Math.max(clock.wallTime() - lastUpdatedTimestamp, 0), delayMs);
        return MathExt.scale(timeDifference, 0.0, delayMs, MIN_DELAYED_SCORE, MAX_DELAYED_SCORE);
    }

    @Override
    public double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        return Math.min(1.0, (subResourcesUsed + subResourcesNeeded + 1.0) / (subResourcesLimit + 1));
    }
}
