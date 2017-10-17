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

import com.netflix.fenzo.queues.tiered.TieredQueueSlas;
import rx.Observable;

/**
 * {@link TierSlaUpdater} provides tier SLA data for Fenzo.
 */
public interface TierSlaUpdater {

    /**
     * On subscription emits immediately one item with current SLA, and another one for each SLA configuration
     * change.
     */
    Observable<TieredQueueSlas> tieredQueueSlaUpdates();
}
