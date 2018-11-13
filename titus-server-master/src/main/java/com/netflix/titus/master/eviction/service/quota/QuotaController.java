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

package com.netflix.titus.master.eviction.service.quota;

import java.util.Optional;

/**
 * An extension of {@link QuotaTracker} which allows the quota to be directly consumed.
 */
public interface QuotaController<DESCRIPTOR> extends QuotaTracker {

    /**
     * Returns quota consumption result for the given task. As {@link #getQuota()} is an aggregate, it is
     * possible that the quota value is greater than zero, but the task consumption fails. This may happen if there
     * are task level restrictions.
     */
    ConsumptionResult consume(String taskId);

    @Override
    default Optional<String> explainRestrictions(String taskId) {
        ConsumptionResult consumptionResult = consume(taskId);
        if (consumptionResult.isApproved()) {
            giveBackConsumedQuota(taskId);
            return Optional.empty();
        }
        return consumptionResult.getRejectionReason();
    }

    /**
     * Return quota, which was previously consumed by the given task.
     */
    void giveBackConsumedQuota(String taskId);

    /**
     * Produces a new {@link QuotaController} instance with the updated configuration. The state from the current
     * instance is carried over with possible adjustments. For example, if the {@link QuotaController} tracks
     * how many containers can be terminated per hour, the number of terminated containers within the last hour
     * would be copied into the new {@link QuotaController} instance.
     */
    default QuotaController<DESCRIPTOR> update(DESCRIPTOR newDescriptor) {
        throw new IllegalStateException("Not directly updatable quota controller: type=" + getClass().getSimpleName());
    }
}
