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

package com.netflix.titus.api.scheduler.service;

import java.util.List;

import com.netflix.titus.api.scheduler.model.SystemSelector;
import com.netflix.titus.api.scheduler.model.SystemSelector;
import rx.Completable;

/**
 * Service facade into the scheduler component
 */
public interface SchedulerService {

    /**
     * @return all system selectors
     */
    List<SystemSelector> getSystemSelectors();

    /**
     * Get a system selector by id.
     *
     * @param id the system selector id
     * @throws SchedulerException {@link SchedulerException.ErrorCode#SystemSelectorNotFound} if the system selector is not found
     */
    SystemSelector getSystemSelector(String id);

    /**
     * Create a system selector.
     *
     * @param systemSelector the system selector
     * @throws SchedulerException {@link SchedulerException.ErrorCode#SystemSelectorAlreadyExists} if the system selector already exists
     */
    Completable createSystemSelector(SystemSelector systemSelector);

    /**
     * Updates a system selector.
     *
     * @param id             the system selector id
     * @param systemSelector the system selector
     * @throws SchedulerException {@link SchedulerException.ErrorCode#SystemSelectorNotFound} if the system selector is not found
     */
    Completable updateSystemSelector(String id, SystemSelector systemSelector);

    /**
     * Deletes a system selector.
     *
     * @param id the system selector id
     * @throws SchedulerException {@link SchedulerException.ErrorCode#SystemSelectorNotFound} if the system selector is not found
     */
    Completable deleteSystemSelector(String id);
}
