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

package com.netflix.titus.master.scheduler;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import com.netflix.titus.master.scheduler.fitness.networkinterface.TitusNetworkInterfaceFitnessEvaluator;

@Configuration(prefix = "titus.scheduler")
public interface SchedulerConfiguration {

    /**
     * @return Sleep interval between consecutive scheduler iterations
     */
    @DefaultValue("500")
    long getSchedulerIterationIntervalMs();

    @DefaultValue("true")
    boolean isSchedulerEnabled();

    /**
     * @return the maximum amount of concurrent threads to use while computing scheduler placements
     */
    @DefaultValue("8")
    int getSchedulerMaxConcurrent();

    /**
     * @return whether or not to limit concurrent task launches on a node
     */
    @DefaultValue("true")
    boolean isGlobalTaskLaunchingConstraintEvaluatorEnabled();

    /**
     * Option used by component {@link TitusNetworkInterfaceFitnessEvaluator}.
     *
     * @return whether or not to use the default fenzo network interface allocation strategy.
     */
    @DefaultValue("false")
    boolean isFenzoNetworkInterfaceAllocationEnabled();

    /**
     * Option used by component {@link TitusNetworkInterfaceFitnessEvaluator}.
     *
     * @return whether or not to use an optimizing algorithm for network interface allocation
     */
    @DefaultValue("false")
    boolean isOptimizingNetworkInterfaceAllocationEnabled();

    /**
     * An option to enable spreading for service jobs in the critical tier.
     *
     * @return whether or not to prefer spreading for service jobs in the critical tier.
     */
    @DefaultValue("true")
    boolean isCriticalServiceJobSpreadingEnabled();

    /**
     * @return whether or not to use system selectors
     */
    @DefaultValue("false")
    boolean isSystemSelectorsEnabled();

    @DefaultValue("true")
    boolean isExitUponFenzoSchedulingErrorEnabled();

    /**
     * An option to enable fenzo downscaling of agents.
     *
     * @return whether or not fenzo should downscale agents.
     */
    @DefaultValue("true")
    boolean isFenzoDownScalingEnabled();

    @DefaultValue("30000")
    long getTierSlaUpdateIntervalMs();

    /**
     * TODO: Remove this property once optimizing shortfall evaluator stabilizes
     * Use the aggressive shortfall evaluator by default.
     */
    @DefaultValue("false")
    boolean isOptimizingShortfallEvaluatorEnabled();

    @DefaultValue("0")
    int getDelayAutoScaleUpBySecs();

    @DefaultValue("0")
    int getDelayAutoScaleDownBySecs();

    /**
     * Return the attribute name to use to get the instance group id
     */
    @DefaultValue("asg")
    String getInstanceGroupAttributeName();

    /**
     * Return the attribute name to use to get the instance id
     */
    @DefaultValue("id")
    String getInstanceAttributeName();

    /**
     * Return the amount of time in milliseconds that is preferred before re-using a network interface. This delay
     * only impacts the score during network interface allocation, but will not prevent a network interface from being chosen.
     */
    @DefaultValue("300000")
    long getPreferredNetworkInterfaceDelayMs();
}