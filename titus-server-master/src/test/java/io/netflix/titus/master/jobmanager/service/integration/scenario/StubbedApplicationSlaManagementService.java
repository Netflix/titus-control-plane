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

package io.netflix.titus.master.jobmanager.service.integration.scenario;

import java.util.Collection;
import java.util.Collections;

import io.netflix.titus.api.model.ApplicationSLA;
import io.netflix.titus.api.model.ResourceDimension;
import io.netflix.titus.api.model.Tier;
import io.netflix.titus.master.service.management.ApplicationSlaManagementService;
import rx.Observable;

class StubbedApplicationSlaManagementService implements ApplicationSlaManagementService {

    private static final ApplicationSLA DEFAULT = new ApplicationSLA(
            "DEFAULT",
            Tier.Flex,
            ResourceDimension.newBuilder().withCpus(16).withMemoryMB(32 * 1024).withNetworkMbs(4096).withDiskMB(100 * 1024).build(),
            10
    );

    @Override
    public Collection<ApplicationSLA> getApplicationSLAs() {
        return Collections.singletonList(DEFAULT);
    }

    @Override
    public ApplicationSLA getApplicationSLA(String applicationName) {
        return applicationName.equals("DEFAULT") ? DEFAULT : null;
    }

    @Override
    public Observable<Void> addApplicationSLA(ApplicationSLA applicationSLA) {
        return Observable.empty();
    }

    @Override
    public Observable<Void> removeApplicationSLA(String applicationName) {
        return Observable.empty();
    }
}
