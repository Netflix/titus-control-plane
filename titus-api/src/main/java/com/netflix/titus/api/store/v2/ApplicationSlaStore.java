/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.store.v2;

import javax.annotation.concurrent.ThreadSafe;

import com.netflix.titus.api.model.ApplicationSLA;
import rx.Observable;

/**
 */
@ThreadSafe
public interface ApplicationSlaStore {

    Observable<Void> create(ApplicationSLA applicationSLA);

    Observable<ApplicationSLA> findAll();

    /**
     * This API retrieves all instances of {@link ApplicationSLA} associated with the given schedulerName.
     * <p>
     * In case the schedulerName} is null or empty string, all {@link ApplicationSLA}
     * instances independent of their associated scheduler is will be returned.
     * </p>
     * @param schedulerName name of the scheduler managing this {@link ApplicationSLA}
     * @return
     */
    Observable<ApplicationSLA> findBySchedulerName(String schedulerName);

    Observable<ApplicationSLA> findByName(String applicationName);

    Observable<Void> remove(String applicationName);
}
