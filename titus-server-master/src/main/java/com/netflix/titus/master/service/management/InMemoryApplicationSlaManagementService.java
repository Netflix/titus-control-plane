/*
 * Copyright 2022 Netflix, Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.api.model.ResourceDimension;
import com.netflix.titus.api.model.Tier;
import com.netflix.titus.api.service.TitusServiceException;
import rx.Observable;

@Singleton
public class InMemoryApplicationSlaManagementService implements ApplicationSlaManagementService {

    private final ConcurrentMap<String, ApplicationSLA> capacityGroups = new ConcurrentHashMap<>();

    @Inject
    public InMemoryApplicationSlaManagementService(CapacityManagementConfiguration configuration) {
        ApplicationSLA defaultCapacityGroup = buildDefaultApplicationSLA(configuration);
        capacityGroups.put(defaultCapacityGroup.getAppName(), defaultCapacityGroup);
    }

    private ApplicationSLA buildDefaultApplicationSLA(CapacityManagementConfiguration configuration) {
        CapacityManagementConfiguration.ResourceDimensionConfiguration rdConf = configuration.getDefaultApplicationResourceDimension();
        return ApplicationSLA.newBuilder()
                .withAppName(ApplicationSlaManagementService.DEFAULT_APPLICATION)
                .withTier(Tier.Flex)
                .withSchedulerName(configuration.getDefaultSchedulerName())
                .withResourcePool("")
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

    @Override
    public Collection<ApplicationSLA> getApplicationSLAs() {
        return new ArrayList<>(capacityGroups.values());
    }

    @Override
    public Collection<ApplicationSLA> getApplicationSLAsForScheduler(String schedulerName) {
        return capacityGroups.values().stream()
                .filter(c -> schedulerName.equals(c.getSchedulerName()))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ApplicationSLA> findApplicationSLA(String applicationName) {
        return Optional.ofNullable(capacityGroups.get(applicationName));
    }

    @Override
    public ApplicationSLA getApplicationSLA(String applicationName) {
        return findApplicationSLA(applicationName).orElse(null);
    }

    @Override
    public Observable<Void> addApplicationSLA(ApplicationSLA applicationSLA) {
        return Observable.defer(() -> {
            capacityGroups.put(applicationSLA.getAppName(), applicationSLA);
            return Observable.empty();
        });
    }

    @Override
    public Observable<Void> removeApplicationSLA(String applicationName) {
        return Observable.defer(() -> {
            capacityGroups.remove(applicationName);
            return Observable.empty();
        });
    }
}
