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

package com.netflix.titus.api;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

/**
 * This configuration interface is a centralized store for all feature flags. Putting all feature flags in one place
 * should improve the project maintenance.
 */
@Configuration(prefix = "titus.feature")
public interface FeatureActivationConfiguration {

    /**
     * This flag enables the integration between the Titus Gateway and Task Relocation components.
     * <p>
     * This change was introduced in Q4/2018. The feature flag should be removed by the end of Q2/2019.
     */
    @DefaultValue("true")
    boolean isMergingTaskMigrationPlanInGatewayEnabled();

    /**
     * Toggle validation of compatibility between Jobs when moving tasks across them. It is useful during emergencies to
     * force tasks to be moved, but it adds risk of causing inconsistencies in Jobs with incompatible tasks. Use with
     * care.
     */
    @DefaultValue("true")
    boolean isMoveTaskValidationEnabled();

    /**
     * Enable binpacking of tasks based on how hard they are to relocate
     */
    @DefaultValue("true")
    boolean isRelocationBinpackingEnabled();

    /**
     * Enable shared informer on Titus gateway
     */
    @DefaultValue("false")
    boolean isInjectingContainerStatesEnabled();

    /**
     * This config controls how tasks in Started state are selected for termination for service jobs.
     * Default (value false) approach prefers to terminate tasks on an agent with the most number of tasks for a
     * given service job.
     * When this config is set to true, we will prefer termination of tasks on agents with the smallest number first.
     * This approach could expedite scale down of nodes naturally when service jobs reduce in size due to
     * auto-scaling during low utilization periods.
     * @return config to flip service job task termination in bin packing friendly way.
     */
    @DefaultValue("false")
    boolean isServiceJobTaskTerminationFavorBinPackingEnabled();
}
