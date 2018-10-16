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

import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.guice.annotation.Activator;
import com.netflix.titus.master.VirtualMachineMasterService;
import com.netflix.titus.master.scheduler.SchedulingService;

/**
 * Helper class providing an extra indirection step for proper activation order.
 */
@Singleton
public class VirtualMachineMasterServiceActivator {

    private final VirtualMachineMasterService virtualMachineMasterService;

    @Inject
    public VirtualMachineMasterServiceActivator(VirtualMachineMasterService virtualMachineMasterService,
                                                SchedulingService schedulingService) {
        this.virtualMachineMasterService = virtualMachineMasterService;
    }

    @Activator
    public void enterActiveMode() {
        ((VirtualMachineMasterServiceMesosImpl) virtualMachineMasterService).enterActiveMode();
    }
}
