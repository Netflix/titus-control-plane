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
import com.netflix.titus.api.connector.cloud.InstanceCloudConnector;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.connector.AbstractSimulatedInstanceCloudConnectorTest;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import com.netflix.titus.testkit.util.NetworkExt;
import org.junit.After;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class SimulatedRemoteInstanceCloudConnectorTest extends AbstractSimulatedInstanceCloudConnectorTest {

    private LifecycleInjector injector;

    @Override
    protected InstanceCloudConnector setup(SimulatedCloud cloud) {
        int grpcPort = NetworkExt.findUnusedPort();
        this.injector = RemoteConnectorUtil.createSimulatedCloudGrpcServer(cloud, grpcPort);
        return new SimulatedRemoteInstanceCloudConnector(RemoteConnectorUtil.newConnectorConfiguration(grpcPort));
    }

    @After
    public void tearDown() {
        if (injector != null) {
            injector.close();
        }
    }
}