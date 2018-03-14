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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.DefaultPreferentialNamedConsumableResourceEvaluator;
import com.netflix.fenzo.PreferentialNamedConsumableResourceEvaluator;
import io.netflix.titus.common.runtime.TitusRuntime;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import io.netflix.titus.master.scheduler.resourcecache.AgentResourceCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network interface fitness evaluator. Two strategies are applied depending on the dynamic configuration
 * setting ({@link SchedulerConfiguration#isOptimizingNetworkInterfaceAllocationEnabled()}):
 *
 * @see SimpleNetworkInterfaceFitnessEvaluator
 * @see OptimizedNetworkInterfaceFitnessEvaluator
 */

@Singleton
public class TitusNetworkInterfaceFitnessEvaluator implements PreferentialNamedConsumableResourceEvaluator {

    private static final Logger logger = LoggerFactory.getLogger(TitusNetworkInterfaceFitnessEvaluator.class);

    private final SchedulerConfiguration configuration;
    private final PreferentialNamedConsumableResourceEvaluator optimizedFitnessEvaluator;
    private final PreferentialNamedConsumableResourceEvaluator simpleFitnessEvaluator;

    @Inject
    public TitusNetworkInterfaceFitnessEvaluator(SchedulerConfiguration configuration,
                                                 AgentResourceCache cache,
                                                 TitusRuntime titusRuntime) {
        this.configuration = configuration;

        this.optimizedFitnessEvaluator = new OptimizedNetworkInterfaceFitnessEvaluator(cache);
        this.simpleFitnessEvaluator = new SimpleNetworkInterfaceFitnessEvaluator(cache, configuration, titusRuntime.getClock());
    }

    @Override
    public double evaluateIdle(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesLimit) {
        double score;
        if (configuration.isFenzoNetworkInterfaceAllocationEnabled()) {
            score = DefaultPreferentialNamedConsumableResourceEvaluator.INSTANCE.evaluateIdle(hostname, resourceName, index, subResourcesNeeded, subResourcesLimit);
        } else if (configuration.isOptimizingNetworkInterfaceAllocationEnabled()) {
            score = optimizedFitnessEvaluator.evaluateIdle(hostname, resourceName, index, subResourcesNeeded, subResourcesLimit);
        } else {
            score = simpleFitnessEvaluator.evaluateIdle(hostname, resourceName, index, subResourcesNeeded, subResourcesLimit);
        }
        logger.debug("Calculated idle score for hostname: {}, resourceName: {}, index: {}, subResourcesNeeded: {}, subResourcesLimit: {} was {}",
                hostname, resourceName, index, subResourcesNeeded, subResourcesLimit, score);
        return score;
    }

    @Override
    public double evaluate(String hostname, String resourceName, int index, double subResourcesNeeded, double subResourcesUsed, double subResourcesLimit) {
        double score;
        if (configuration.isFenzoNetworkInterfaceAllocationEnabled()) {
            score = DefaultPreferentialNamedConsumableResourceEvaluator.INSTANCE.evaluate(hostname, resourceName, index, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
        } else if (configuration.isOptimizingNetworkInterfaceAllocationEnabled()) {
            score = optimizedFitnessEvaluator.evaluate(hostname, resourceName, index, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
        } else {
            score = simpleFitnessEvaluator.evaluate(hostname, resourceName, index, subResourcesNeeded, subResourcesUsed, subResourcesLimit);
        }
        logger.debug("Calculated score for hostname: {}, resourceName: {}, index: {}, subResourcesNeeded: {}, subResourcesUsed: {}, subResourcesLimit: {} was {}",
                hostname, resourceName, index, subResourcesNeeded, subResourcesUsed, subResourcesLimit, score);
        return score;
    }
}
