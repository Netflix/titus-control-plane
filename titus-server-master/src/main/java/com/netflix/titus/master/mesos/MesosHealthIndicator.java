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

package com.netflix.titus.master.mesos;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.runtime.health.api.Health;
import com.netflix.runtime.health.api.HealthIndicator;
import com.netflix.runtime.health.api.HealthIndicatorCallback;
import com.netflix.titus.master.VirtualMachineMasterService;

/**
 * For TitusMaster leader, check if it is connected to Mesos.
 */
@Singleton
class MesosHealthIndicator implements HealthIndicator {

    private final VirtualMachineMasterService vmService;

    @Inject
    public MesosHealthIndicator(VirtualMachineMasterService vmService) {
        this.vmService = vmService;
    }

    @Override
    public void check(HealthIndicatorCallback healthCallback) {
        if (vmService instanceof VirtualMachineMasterServiceMesosImpl) {
            VirtualMachineMasterServiceMesosImpl mesosVmService = (VirtualMachineMasterServiceMesosImpl) vmService;
            Optional<Boolean> status = mesosVmService.isConnectedToMesos();
            if (status.isPresent()) {
                if (status.get()) {
                    healthCallback.inform(Health.healthy()
                            .withDetail("implementation", vmService.getClass())
                            .withDetail("connectionStatus", "connected")
                            .build());

                } else {
                    healthCallback.inform(Health.unhealthy()
                            .withDetail("implementation", vmService.getClass())
                            .withDetail("connectionStatus", "disconnected (leader)")
                            .build());
                }
            } else {
                healthCallback.inform(Health.healthy()
                        .withDetail("implementation", vmService.getClass())
                        .withDetail("connectionStatus", "disconnected (not a leader)")
                        .build());
            }
        } else {
            healthCallback.inform(Health.healthy().withDetail("implementation", vmService.getClass()).build());
        }
    }
}
