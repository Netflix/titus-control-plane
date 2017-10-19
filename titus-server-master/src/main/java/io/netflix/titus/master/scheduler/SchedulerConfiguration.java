/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.scheduler;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.scheduler")
public interface SchedulerConfiguration {

    /**
     * @return Sleep interval between consecutive scheduler iterations
     */
    @DefaultValue("100")
    long getSchedulerIterationIntervalMs();

    @DefaultValue("true")
    boolean isSchedulerEnabled();

    /**
     * @return whether or not to limit concurrent task launches on a node
     */
    @DefaultValue("false")
    boolean isGlobalTaskLaunchingConstraintEvaluatorEnabled();

    @DefaultValue("true")
    boolean isExitUponFenzoSchedulingErrorEnabled();

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
     * Return the attribute name to use from the agent when comparing against instance groups
     */
    @DefaultValue("asg")
    String getInstanceGroupAttributeName();
}