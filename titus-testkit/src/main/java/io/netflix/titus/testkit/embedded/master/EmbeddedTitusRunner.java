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

import com.netflix.governator.InjectorBuilder;
import com.netflix.governator.LifecycleInjector;
import io.netflix.titus.common.runtime.internal.DefaultTitusRuntime;
import io.netflix.titus.testkit.embedded.cloud.EmbeddedCloudModule;
import io.netflix.titus.testkit.embedded.cloud.SimulatedCloud;

public class EmbeddedTitusRunner {

    public static void main(String[] args) throws InterruptedException {
        LifecycleInjector injector = InjectorBuilder.fromModule(new EmbeddedCloudModule()).createInjector();
        SimulatedCloud cloud = injector.getInstance(SimulatedCloud.class);

        System.setProperty(DefaultTitusRuntime.FIT_ACTIVATION_PROPERTY, "true");
        EmbeddedTitusMaster.Builder builder = EmbeddedTitusMaster.aTitusMaster()
                .withSimulatedCloud(cloud)
                .withCassadraV3JobStore()
                .withProperty("titus.master.grpcServer.v3EnabledApps", ".*")
                .withApiPort(8080)
                .withGrpcPort(8090);

        EmbeddedTitusMaster titusMaster = builder.build();
        titusMaster.boot();
        System.out.println("TitusMaster started");
        Thread.sleep(24 * 60 * 60 * 1000L);
    }
}
