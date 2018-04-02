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

package com.netflix.titus.master.scheduler.service;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.master.scheduler.systemselector.SystemSelectorService;
import com.netflix.titus.api.scheduler.model.SystemSelector;
import com.netflix.titus.api.scheduler.service.SchedulerService;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.common.util.guice.annotation.ProxyConfiguration;
import com.netflix.titus.master.scheduler.systemselector.SystemSelectorService;
import rx.Completable;

import static com.netflix.titus.common.util.guice.ProxyType.ActiveGuard;
import static com.netflix.titus.common.util.guice.ProxyType.Logging;
import static com.netflix.titus.common.util.guice.ProxyType.Spectator;

@Singleton
@ProxyConfiguration(types = {Logging, Spectator, ActiveGuard})
public class DefaultSchedulerService implements SchedulerService {
    private final SystemSelectorService systemSelectorService;

    @Inject
    public DefaultSchedulerService(SystemSelectorService systemSelectorService) {
        this.systemSelectorService = systemSelectorService;
    }

    @Activator
    public void enterActiveMode() {
        // We need this empty method, to mark this service as activated.
    }


    @Override
    public List<SystemSelector> getSystemSelectors() {
        return systemSelectorService.getSystemSelectors();
    }

    @Override
    public SystemSelector getSystemSelector(String id) {
        return systemSelectorService.getSystemSelector(id);
    }

    @Override
    public Completable createSystemSelector(SystemSelector systemSelector) {
        return systemSelectorService.createSystemSelector(systemSelector);
    }

    @Override
    public Completable updateSystemSelector(String id, SystemSelector systemSelector) {
        return systemSelectorService.updateSystemSelector(id, systemSelector);
    }

    @Override
    public Completable deleteSystemSelector(String id) {
        return systemSelectorService.deleteSystemSelector(id);
    }
}
