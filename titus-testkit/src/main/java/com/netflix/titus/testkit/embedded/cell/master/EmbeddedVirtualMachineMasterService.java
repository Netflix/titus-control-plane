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

package com.netflix.titus.testkit.embedded.cell.master;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.inject.Injector;
import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.mesos.MesosConfiguration;
import com.netflix.titus.master.mesos.MesosSchedulerDriverFactory;
import com.netflix.titus.master.mesos.VirtualMachineMasterServiceMesosImpl;
import com.netflix.titus.master.scheduler.SchedulerConfiguration;
import com.netflix.titus.testkit.embedded.cloud.connector.local.SimulatedLocalMesosSchedulerDriver;

@Singleton
public class EmbeddedVirtualMachineMasterService extends VirtualMachineMasterServiceMesosImpl {

    @Inject
    public EmbeddedVirtualMachineMasterService(MasterConfiguration config,
                                               SchedulerConfiguration schedulerConfiguration,
                                               MesosConfiguration mesosConfiguration,
                                               MesosSchedulerDriverFactory mesosSchedulerDriverFactory,
                                               Injector injector,
                                               TitusRuntime titusRuntime) {
        super(config, schedulerConfiguration, mesosConfiguration, mesosSchedulerDriverFactory, injector, titusRuntime);
    }

    SimulatedLocalMesosSchedulerDriver getSimulatedMesosDriver() {
        return (SimulatedLocalMesosSchedulerDriver) getMesosDriver();
    }
}
