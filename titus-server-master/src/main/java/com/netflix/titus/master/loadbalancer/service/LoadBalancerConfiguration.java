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

package com.netflix.titus.master.loadbalancer.service;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;

@Configuration(prefix = "titus.master.loadBalancer")
public interface LoadBalancerConfiguration {

    @DefaultValue("true")
    boolean isEngineEnabled();

    @DefaultValue("4")
    long getRateLimitBurst();

    @DefaultValue("20")
    long getRateLimitRefillPerSec();

    /**
     * How long the reconciliation logic will ignore a particular target after an update has been enqueued for it.
     * This should be higher than the expected max propagation delay for updates, so the reconciliation loop is able
     * to detect changes made to loadbalancers.
     */
    @DefaultValue("120000")
    long getCooldownPeriodMs();

    /**
     * Max execution time for a full reconciliation.
     */
    @DefaultValue("90000")
    long getReconciliationTimeoutMs();

    /**
     * Delay between full reconciliation runs.
     */
    @DefaultValue("30000")
    long getReconciliationDelayMs();

    /**
     * Minimum time that items are held in a buffer for batching.
     */
    @DefaultValue("1000")
    long getMinTimeMs();

    /**
     * Maximum time that items are held in a buffer for batching (times are increased with exponential backoff).
     */
    @DefaultValue("60000")
    long getMaxTimeMs();

    /**
     * Size of the time bucket to group batches when sorting them by timestamp, so bigger batches in the same bucket
     * are picked first.
     * <p>
     * This provides a knob to control how to favor larger batches vs older batches first.
     */
    @DefaultValue("5000")
    long getBucketSizeMs();
}
