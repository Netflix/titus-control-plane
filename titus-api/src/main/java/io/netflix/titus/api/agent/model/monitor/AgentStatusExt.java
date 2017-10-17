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

package io.netflix.titus.api.agent.model.monitor;

import java.util.Collection;
import java.util.Iterator;

import io.netflix.titus.api.agent.model.monitor.AgentStatus.AgentStatusCode;
import rx.Scheduler;

/**
 * A collection of helper methods operating on {@link AgentStatus} objects.
 */
public final class AgentStatusExt {

    private AgentStatusExt() {
    }

    /**
     * Compute effective disable time by compensating for the elapsed time. {@link Scheduler} is our source of time.
     */
    public static long effectiveDisableTime(AgentStatus agentStatus, Scheduler scheduler) {
        long effective = agentStatus.getDisableTime() - (scheduler.now() - agentStatus.getEmitTime());
        return effective > 0 ? effective : 0;
    }

    /**
     * {@link AgentStatusCode#Healthy} is never expired. {@link AgentStatusCode#Unhealthy} is expired
     * when its effective disable time is <= 0.
     */
    public static boolean isExpired(AgentStatus status, Scheduler scheduler) {
        return status.getStatusCode() == AgentStatusCode.Unhealthy && effectiveDisableTime(status, scheduler) <= 0;
    }

    /**
     * Checks if the two given status values are different with respect to the status code and the effective disable time.
     */
    public static boolean isDifferent(AgentStatus newStatus, AgentStatus lastStatus, Scheduler scheduler) {
        if (lastStatus == null) {
            return true;
        }
        if (newStatus.getStatusCode() == AgentStatusCode.Healthy) {
            return lastStatus.getStatusCode() != AgentStatusCode.Healthy;
        }
        return effectiveDisableTime(newStatus, scheduler) != effectiveDisableTime(lastStatus, scheduler);
    }

    /**
     * Given collection of {@link AgentStatus} values:
     * <ul>
     * <li>if all values are {@link AgentStatusCode#Healthy}, return one of them</li>
     * <li>if some values are {@link AgentStatusCode#Unhealthy}, return the one with the largest effective disable time</li>
     * </ul>
     */
    public static AgentStatus evaluateEffectiveStatus(Collection<AgentStatus> values, Scheduler scheduler) {
        Iterator<AgentStatus> it = values.iterator();
        AgentStatus effectiveStatus = it.next(); // There is always at least one item
        while (it.hasNext()) {
            effectiveStatus = evaluateNextEffectiveStatus(effectiveStatus, it.next(), scheduler);
        }
        return effectiveStatus;
    }

    private static AgentStatus evaluateNextEffectiveStatus(AgentStatus first, AgentStatus second, Scheduler scheduler) {
        if (first.getStatusCode() == AgentStatusCode.Healthy) {
            return second;
        }
        if (second.getStatusCode() == AgentStatusCode.Healthy) {
            return first;
        }
        return effectiveDisableTime(first, scheduler) < effectiveDisableTime(second, scheduler) ? second : first;
    }
}
