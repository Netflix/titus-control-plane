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

package com.netflix.titus.master.service.management.internal;

import java.util.Collection;
import java.util.Collections;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.master.service.management.ApplicationSlaManagementService;
import com.netflix.titus.master.service.management.CapacityMonitoringService;
import com.netflix.titus.master.service.management.ManagementSubsystemInitializer;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import rx.Observable;

@Singleton
public class DefaultApplicationSlaManagementService implements ApplicationSlaManagementService {

    private final CapacityMonitoringService capacityMonitoringService;
    private final ApplicationSlaStore storage;

    /**
     * Injecting {@link ManagementSubsystemInitializer} here, to make sure everything is ready, before this service
     * is used.
     */
    @Inject
    public DefaultApplicationSlaManagementService(CapacityMonitoringService capacityMonitoringService,
                                                  ApplicationSlaStore storage,
                                                  ManagementSubsystemInitializer initializer) {
        this.capacityMonitoringService = capacityMonitoringService;
        this.storage = storage;
    }

    @Override
    public Collection<ApplicationSLA> getApplicationSLAs() {
        return storage.findAll().onErrorReturn(t -> null)
                .toList().toBlocking().firstOrDefault(Collections.emptyList());
    }

    @Override
    public ApplicationSLA getApplicationSLA(String applicationName) {
        return storage.findByName(applicationName).onErrorReturn(t -> null).toBlocking().firstOrDefault(null);
    }

    @Override
    public Observable<Void> addApplicationSLA(ApplicationSLA applicationSLA) {
        // We trigger refresh, but not wait for the result, as we only care that first part (create) succeeded.
        return storage.create(applicationSLA).doOnCompleted(() -> capacityMonitoringService.refresh().subscribe());
    }

    @Override
    public Observable<Void> removeApplicationSLA(String applicationName) {
        if (applicationName.equals(DEFAULT_APPLICATION)) {
            return Observable.error(new IllegalArgumentException(DEFAULT_APPLICATION + " cannot be removed"));
        }
        // We trigger refresh, but not wait for the result, as we only care that first part (remove) succeeded.
        return storage.remove(applicationName).doOnCompleted(() -> capacityMonitoringService.refresh().subscribe());
    }
}
