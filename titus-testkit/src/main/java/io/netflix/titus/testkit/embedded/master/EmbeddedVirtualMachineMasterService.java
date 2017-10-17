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

package io.netflix.titus.testkit.embedded.master;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.spectator.api.Registry;
import io.netflix.titus.api.jobmanager.model.job.sanitizer.JobConfiguration;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.master.MasterDescription;
import io.netflix.titus.master.mesos.MesosMasterResolver;
import io.netflix.titus.master.mesos.MesosSchedulerDriverFactory;
import io.netflix.titus.master.mesos.VirtualMachineMasterServiceMesosImpl;
import io.netflix.titus.master.scheduler.SchedulerConfiguration;
import io.netflix.titus.master.zookeeper.ZookeeperPaths;
import io.netflix.titus.testkit.embedded.cloud.agent.SimulatedMesosSchedulerDriver;

@Singleton
public class EmbeddedVirtualMachineMasterService extends VirtualMachineMasterServiceMesosImpl {

    private static final MesosMasterResolver FIXED_RESOLVER = new MesosMasterResolver() {
        @Override
        public Optional<String> resolveCanonical() {
            return Optional.of("mesos-server");
        }

        @Override
        public Optional<InetSocketAddress> resolveLeader() {
            return Optional.of(new InetSocketAddress("mesos-server", 5050));
        }

        @Override
        public List<InetSocketAddress> resolveMesosAddresses() {
            return Collections.singletonList(new InetSocketAddress("mesos-server", 5050));
        }
    };

    @Inject
    public EmbeddedVirtualMachineMasterService(MasterConfiguration config,
                                               SchedulerConfiguration schedulerConfiguration,
                                               JobConfiguration jobConfiguration,
                                               MasterDescription masterDescription,
                                               ZookeeperPaths zkPaths,
                                               MesosSchedulerDriverFactory mesosSchedulerDriverFactory,
                                               Registry metricsRegistry) {
        super(config, schedulerConfiguration, jobConfiguration, masterDescription, zkPaths, FIXED_RESOLVER, mesosSchedulerDriverFactory, metricsRegistry);
    }

    SimulatedMesosSchedulerDriver getSimulatedMesosDriver() {
        return (SimulatedMesosSchedulerDriver) getMesosDriver();
    }
}
