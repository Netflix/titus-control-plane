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

package com.netflix.titus.testkit.embedded.cloud.connector.remote;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.master.mesos.MesosSchedulerDriverFactory;
import io.grpc.Channel;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

@Singleton
public class SimulatedRemoteMesosSchedulerDriverFactory implements MesosSchedulerDriverFactory {

    private final Channel channel;
    private final Protos.MasterInfo masterInfo;
    private final TitusRuntime titusRuntime;

    @Inject
    public SimulatedRemoteMesosSchedulerDriverFactory(@Named(SimulatedRemoteInstanceCloudConnector.SIMULATED_CLOUD) Channel channel, TitusRuntime titusRuntime) {
        this.titusRuntime = titusRuntime;

        this.masterInfo = Protos.MasterInfo.newBuilder()
                .setId("MasterId#Simulated")
                .setAddress(Protos.Address.newBuilder().setHostname("simulated").setPort(0))
                .setHostname("simulated")
                .setIp(0)
                .setPort(0)
                .setVersion("1.2.simulated")
                .build();

        this.channel = channel;
    }

    @Override
    public SchedulerDriver createDriver(Protos.FrameworkInfo framework, String mesosMaster, Scheduler scheduler) {
        return new SimulatedRemoteMesosSchedulerDriver(masterInfo, channel, scheduler, titusRuntime);
    }
}
