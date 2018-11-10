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

package com.netflix.titus.api.eviction.model.event;

import com.netflix.titus.api.eviction.model.EvictionQuota;

public abstract class EvictionEvent {

    public static EvictionSnapshotEndEvent newSnapshotEndEvent() {
        return EvictionSnapshotEndEvent.getInstance();
    }

    public static EvictionQuotaEvent newQuotaEvent(EvictionQuota evictionQuota) {
        return new EvictionQuotaEvent(evictionQuota);
    }

    public static TaskTerminationEvent newSuccessfulTaskTerminationEvent(String taskId, String reason) {
        return new TaskTerminationEvent(taskId, reason);
    }

    public static TaskTerminationEvent newFailedTaskTerminationEvent(String taskId, String reason, Throwable error) {
        return new TaskTerminationEvent(taskId, reason, error);
    }
}
