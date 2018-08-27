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

package com.netflix.titus.master.clusteroperations;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.clusterOperations")
public interface ClusterOperationsConfiguration {

    /**
     * @return whether or not the cluster agent auto scaler should auto scale agents.
     */
    @DefaultValue("true")
    boolean isAutoScalingAgentsEnabled();

    /**
     * @return the the primary instance type for the Critical tier.
     */
    @DefaultValue("m4.16xlarge")
    String getCriticalPrimaryInstanceType();

    /**
     * @return the minimum number of idle agents to keep for the Critical tier.
     */
    @DefaultValue("0")
    int getCriticalMinIdle();

    /**
     * @return the maximum number of idle agents to keep for the Critical tier.
     */
    @DefaultValue("10")
    int getCriticalMaxIdle();

    /**
     * @return the scale up cool down period for the Critical tier.
     */
    @DefaultValue("600000")
    long getCriticalScaleUpCoolDownMs();

    /**
     * @return the scale down cool down period for the Critical tier.
     */
    @DefaultValue("600000")
    long getCriticalScaleDownCoolDownMs();

    /**
     * @return the amount of time the auto scaler can wait for an individual Critical tier task before scaling up instances
     * for it.
     */
    @DefaultValue("60000")
    long getCriticalTaskSloMs();

    /**
     * @return the amount of time after the agent launch time the auto scaler waits before classifying a Critical tier agent as idle.
     */
    @DefaultValue("900000")
    long getCriticalIdleInstanceGracePeriodMs();

    /**
     * @return the the primary instance type for the Critical tier.
     */
    @DefaultValue("r4.16xlarge")
    String getFlexPrimaryInstanceType();

    /**
     * @return the minimum number of idle agents to keep for the Flex tier.
     */
    @DefaultValue("5")
    int getFlexMinIdle();

    /**
     * @return the maximum number of idle agents to keep for the Flex tier.
     */
    @DefaultValue("10")
    int getFlexMaxIdle();

    /**
     * @return the scale up cool down period for the Flex tier.
     */
    @DefaultValue("600000")
    long getFlexScaleUpCoolDownMs();

    /**
     * @return the scale down cool down period for the Flex tier.
     */
    @DefaultValue("600000")
    long getFlexScaleDownCoolDownMs();

    /**
     * @return the amount of time the auto scaler can wait for an individual Flex tier task before scaling up instances
     * for it.
     */
    @DefaultValue("300000")
    long getFlexTaskSloMs();

    /**
     * @return the amount of time after the agent launch time the auto scaler waits before classifying a Flex tier agent as idle.
     */
    @DefaultValue("900000")
    long getFlexIdleInstanceGracePeriodMs();

    /**
     * @return the grace period in milliseconds before agents will be removed once in the removable state.
     */
    @DefaultValue("120000")
    long getAgentInstanceRemovableGracePeriodMs();

    /**
     * @return the timeout in milliseconds before agent in the Removable state will lose their OverrideStatus. This will
     * happen automatically if the auto scaler marks an agent as Removable but something ends up being placed on the agent
     * due to synchronization delays.
     */
    @DefaultValue("600000")
    long getAgentInstanceRemovableTimeoutMs();

    /**
     * @return whether or not the cluster agent removers should remove agents.
     */
    @DefaultValue("true")
    boolean isRemovingAgentsEnabled();

    /**
     * @return the grace period in milliseconds before the remover will start removing instances in an instance group.
     */
    @DefaultValue("300000")
    long getInstanceGroupRemovableGracePeriodMs();
}