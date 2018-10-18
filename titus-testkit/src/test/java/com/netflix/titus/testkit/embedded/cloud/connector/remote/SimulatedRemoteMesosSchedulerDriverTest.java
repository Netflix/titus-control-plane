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

import com.netflix.governator.LifecycleInjector;
import com.netflix.titus.common.network.socket.UnusedSocketPortAllocator;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.connector.AbstractSimulatedMesosSchedulerDriverTest;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.junit.After;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class SimulatedRemoteMesosSchedulerDriverTest extends AbstractSimulatedMesosSchedulerDriverTest {

    private LifecycleInjector injector;

    private SimulatedRemoteMesosSchedulerDriverFactory factory;

    @Override
    protected SchedulerDriver setup(SimulatedCloud cloud, Protos.FrameworkInfo framework, Scheduler callbackHandler) {
        int grpcPort = UnusedSocketPortAllocator.global().allocate();
        this.injector = RemoteConnectorUtil.createSimulatedCloudGrpcServer(cloud, grpcPort);

        this.factory = new SimulatedRemoteMesosSchedulerDriverFactory(RemoteConnectorUtil.newConnectorConfiguration(grpcPort), TitusRuntimes.internal());
        return factory.createDriver(framework, "N/A", callbackHandler);
    }

    @After
    public void tearDown() {
        if (factory != null) {
            factory.shutdown();
        }
        if (injector != null) {
            injector.close();
        }
    }
}
