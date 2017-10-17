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

package io.netflix.titus.testkit.embedded.cloud.agent;

import javax.inject.Singleton;

import io.netflix.titus.master.mesos.MesosSchedulerCallbackHandler;
import io.netflix.titus.master.mesos.MesosSchedulerDriverFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

@Singleton
public class SimulatedMesosSchedulerDriverFactory implements MesosSchedulerDriverFactory {
    @Override
    public SchedulerDriver createDriver(Protos.FrameworkInfo framework, String mesosMaster, MesosSchedulerCallbackHandler scheduler) {
        return new SimulatedMesosSchedulerDriver(framework, scheduler);
    }
}
