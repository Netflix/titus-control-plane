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
import com.netflix.titus.common.network.socket.UnusedSocketPortAllocator;
import com.netflix.titus.common.util.ExceptionExt;
import com.netflix.titus.testkit.embedded.cloud.SimulatedCloud;
import com.netflix.titus.testkit.embedded.cloud.connector.AbstractSimulatedInstanceCloudConnectorTest;
import com.netflix.titus.testkit.junit.category.IntegrationTest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.After;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class SimulatedRemoteInstanceCloudConnectorTest extends AbstractSimulatedInstanceCloudConnectorTest {

    private LifecycleInjector injector;
    private ManagedChannel channel;

    @Override
    protected InstanceCloudConnector setup(SimulatedCloud cloud) {
        int grpcPort = UnusedSocketPortAllocator.global().allocate();
        this.injector = RemoteConnectorUtil.createSimulatedCloudGrpcServer(cloud, grpcPort);

        this.channel = ManagedChannelBuilder.forAddress("localhost", grpcPort)
                .usePlaintext()
                .build();
        return new SimulatedRemoteInstanceCloudConnector(channel);
    }

    @After
    public void tearDown() {
        ExceptionExt.silent(channel, c -> channel.shutdownNow());
        ExceptionExt.silent(injector, i -> injector.close());
    }
}