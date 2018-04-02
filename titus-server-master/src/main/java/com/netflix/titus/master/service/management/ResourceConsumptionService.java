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

package com.netflix.titus.master.service.management;

import java.util.Optional;

import com.netflix.titus.master.service.management.ResourceConsumptionEvents.ResourceConsumptionEvent;
import rx.Observable;

/**
 * A service that observers resource utilization by jobs, and checks if configured SLAs are not violated.
 */
public interface ResourceConsumptionService {

    /**
     * Returns current system-level resource consumption. The return object is a composite, that provides
     * further details at tier, capacity group and application levels.
     *
     * @return non-null value if system consumption data are available
     */
    Optional<CompositeResourceConsumption> getSystemConsumption();

    /**
     * Returns an observable that emits an item for each capacity category with its current resource utilization.
     * On subscribe emits a single item for each known category, followed by updates caused by either resource
     * consumption change or SLA update.
     */
    Observable<ResourceConsumptionEvents.ResourceConsumptionEvent> resourceConsumptionEvents();
}
