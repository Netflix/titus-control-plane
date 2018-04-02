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

import rx.Observable;

/**
 * {@link CapacityMonitoringService} is responsible for maintaining Titus agent resource allocations according
 * to the configured SLAs levels. Actual resource allocation is delegated to {@link CapacityAllocationService}.
 */
public interface CapacityMonitoringService {

    /**
     * Schedule re-evaluation of all resource allocations according to the current application SLA agreements.
     *
     * @return observable that when subscribed, evaluates current capacity needs, and update instance allocations.
     * It completes as soon as an update actions are accepted by {@link CapacityAllocationService}, but not
     * necessarily executed yet.
     */
    Observable<Void> refresh();
}
