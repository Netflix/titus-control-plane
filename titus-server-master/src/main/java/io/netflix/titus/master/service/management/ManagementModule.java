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

package io.netflix.titus.master.service.management;

import javax.inject.Singleton;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.netflix.archaius.ConfigProxyFactory;
import io.netflix.titus.master.service.management.internal.DefaultApplicationSlaManagementService;
import io.netflix.titus.master.service.management.internal.DefaultAvailableCapacityService;
import io.netflix.titus.master.service.management.internal.DefaultCapacityMonitoringService;
import io.netflix.titus.master.service.management.internal.DefaultResourceConsumptionService;
import io.netflix.titus.master.service.management.internal.ResourceConsumptionLog;
import io.netflix.titus.master.service.management.internal.SimpleCapacityGuaranteeStrategy;

public class ManagementModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ManagementSubsystemInitializer.class).asEagerSingleton();

        // Capacity management
        bind(CapacityGuaranteeStrategy.class).to(SimpleCapacityGuaranteeStrategy.class);
        bind(ApplicationSlaManagementService.class).to(DefaultApplicationSlaManagementService.class);
        bind(CapacityMonitoringService.class).to(DefaultCapacityMonitoringService.class).asEagerSingleton();
        bind(AvailableCapacityService.class).to(DefaultAvailableCapacityService.class);

        // Resource consumption monitoring
        bind(ResourceConsumptionService.class).to(DefaultResourceConsumptionService.class).asEagerSingleton();
        bind(ResourceConsumptionLog.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public CapacityManagementConfiguration getCapacityManagementConfiguration(ConfigProxyFactory factory) {
        return factory.newProxy(CapacityManagementConfiguration.class);
    }
}
