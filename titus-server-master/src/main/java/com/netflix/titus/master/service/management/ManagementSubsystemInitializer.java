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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.service.management.CapacityManagementConfiguration.ResourceDimensionConfiguration;
import com.netflix.titus.api.store.v2.ApplicationSlaStore;
import com.netflix.titus.api.store.v2.exception.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

/**
 * Boot time initialization logic for the management subsystem. Currently it creates 'DEFAULT' application
 * SLA, if non is defined. The SLA values are read from {@link CapacityManagementConfiguration}.
 */
@Singleton
public class ManagementSubsystemInitializer {

    private static final Logger logger = LoggerFactory.getLogger(ManagementSubsystemInitializer.class);

    private final CapacityManagementConfiguration configuration;
    private final ApplicationSlaStore applicationSlaStore;

    @Inject
    public ManagementSubsystemInitializer(CapacityManagementConfiguration configuration,
                                          ApplicationSlaStore applicationSlaStore) {
        this.configuration = configuration;
        this.applicationSlaStore = applicationSlaStore;
    }

    @Activator
    public Observable<Void> enterActiveMode() {
        logger.info("Entering active mode");
        try {
            ApplicationSLA defaultSLA = applicationSlaStore.findByName(ApplicationSlaManagementService.DEFAULT_APPLICATION).toBlocking().first();
            logger.info("Default application SLA configured as: {}", defaultSLA);
        } catch (NotFoundException e) {
            ApplicationSLA defaultSLA = buildDefaultApplicationSLA(configuration);
            applicationSlaStore.create(defaultSLA).toBlocking().firstOrDefault(null);
            logger.info("Default application SLA not defined; creating it according to the provided configuration: {}", defaultSLA);
        }
        return Observable.empty();
    }

    private ApplicationSLA buildDefaultApplicationSLA(CapacityManagementConfiguration configuration) {
        ResourceDimensionConfiguration rdConf = configuration.getDefaultApplicationResourceDimension();
        return ApplicationSLA.newBuilder()
                .withAppName(ApplicationSlaManagementService.DEFAULT_APPLICATION)
                .withTier(Tier.Flex)
                .withSchedulerName(configuration.getDefaultSchedulerName())
                .withInstanceCount(configuration.getDefaultApplicationInstanceCount())
                .withResourceDimension(ResourceDimension.newBuilder()
                        .withCpus(rdConf.getCPU())
                        .withMemoryMB(rdConf.getMemoryMB())
                        .withDiskMB(rdConf.getDiskMB())
                        .withNetworkMbs(rdConf.getNetworkMbs())
                        .build()
                )
                .build();
    }
}
